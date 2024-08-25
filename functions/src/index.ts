import * as functions from "firebase-functions";
import * as admin from "firebase-admin";
import {auth} from "firebase-admin";
import {DocumentData, DocumentReference, DocumentSnapshot} from '@google-cloud/firestore';
import {log} from "firebase-functions/lib/logger";
import * as util from "util";

const app = admin.initializeApp();
const firestore = app.firestore();

const intervalValueMask = 0x1FFFFFFF;


/**
 * Http Endpoint to create a new group from a given group name.
 * The authenticated user will be added as admin.
 *
 * @param name      the name of the new group
 *
 * @return promise to create the group and add the user as a member
 *
 * @throws HttpsError if user is not authenticated
 */
export const createGroup = functions.https.onCall(async (data, context) => {
    if (!context.auth) {
        // Throwing an HttpsError so that the client gets the error details.
        throw new functions.https.HttpsError('failed-precondition', 'The function must be called while authenticated.');
    }

    const name: string = data.name;

    const groupData = {
        name         : name,
        latestUpdate : new Date(),
        totalExpenses: 0,
        totalPayments: 0,
        numMembers   : 0
    }

    return firestore.collection('groups').add(groupData)
        .then(result => addUserToGroup(result.id, context.auth?.uid!, "admin"));
});

/**
 * Http Endpoint to add an authenticated user to a specific group as a specific role.
 *
 * @param groupId   id of the group the user wants to join.
 * @param role      role (admin or member) of the user in the group
 *
 * @return promise to add the user to the group
 *
 * @throws HttpsError if user is not authenticated
 */
export const joinGroup = functions.https.onCall(async (data, context) => {
    if (!context.auth) {
        // Throwing an HttpsError so that the client gets the error details.
        throw new functions.https.HttpsError('failed-precondition', 'The function must be called while authenticated.');
    }

    const groupId  = data.groupId;
    const role     = data.role;
    const uid      = context.auth.uid;
    return addUserToGroup(groupId, uid, role);
});

/**
 * Adds an user to a group.
 *
 * @param groupId   id of the group the user will be added
 * @param uid       id of the user that will be added to the group
 * @param role      role of the user in the group
 *
 * @return promise to add the user to the group and return the groupId
 */
async function addUserToGroup(groupId: string, uid: string, role: string) {
    const user     = await auth().getUser(uid);
    const groupDoc = await firestore.doc(`groups/${groupId}`).get();

    if (!groupDoc.exists) {
        throw new functions.https.HttpsError('failed-precondition', `Group with id ${groupId} does not exist.`);
    }

    const groupMemberRef  = firestore.doc(`groups/${groupId}/members/${uid}/`);
    const groupMemberData = {
        userName: user.displayName,
        role    : role,
        joinDate: new Date(),
        totalExpenses: 0,
        totalPayments: 0,
    }

    return groupMemberRef.set(groupMemberData).then(_ => groupId);
}

/**
 * Http-Endpoint to remove the authenticated user from a given group.
 *
 * @param groupId id of the group the user wand to leave
 *
 * @return promise removing the user from the group
 *
 * @throws HttpsError if user is not authenticated
 */
export const leaveGroup = functions.https.onCall(async (data, context) => {
    if (!context.auth) {
        // Throwing an HttpsError so that the client gets the error details.
        throw new functions.https.HttpsError('failed-precondition', 'The function must be called while authenticated.');
    }

    const groupId  = data.groupId;
    const uid      = context.auth.uid;

    const groupDoc = await firestore.doc(`groups/${groupId}`).get();
    if (!groupDoc.exists) {
        throw new functions.https.HttpsError('failed-precondition', `Group with id ${groupId} does not exist.`);
    }

    const groupMemberRef = firestore.doc(`groups/${groupId}/members/${uid}/`);

    const rv = await groupMemberRef.delete().then(_ => groupId);

    const groupMembersRef = firestore.collection(`groups/${groupId}/members`);
    const groupMembersCol = await groupMembersRef.get();

    if (groupMembersCol.empty) {
        // "freeze" recurring expenses for abandoned groups
        const groupExpensesRef = firestore.collection(`groups/${groupId}/members`);
        const groupExpensesCol = await groupExpensesRef.get();
        groupExpensesCol.forEach((doc) => {
            return doc.ref?.set({alreadyRecurred: true}, {merge: true});
        })
    }

    return rv;
});

/**
 * Http-Endpoint to kick users from a given group.
 * The requesting user must be admin of the group, to access this operation.
 *
 * @param groupId id of the group the user will be kicked from
 * @param memberId id of the user that will be kicked
 *
 * @return promise to remove the given user from the group.
 *
 * @throws HttpsError if user is not authenticated of if the requesting user is no group admin
 */
export const kickMemberFromGroup = functions.https.onCall(async (data, context) => {
    if (!context.auth) {
        // Throwing an HttpsError so that the client gets the error details.
        throw new functions.https.HttpsError('failed-precondition', 'The function must be called while authenticated.');
    }

    const groupId  = data.groupId;
    const memberId = data.memberId;
    const uid      = context.auth.uid;

    const groupDoc = await firestore.doc(`groups/${groupId}`).get();
    if (!groupDoc.exists) {
        throw new functions.https.HttpsError('failed-precondition', `Group with id ${groupId} does not exist.`);
    }

    const issuerMemberRef = firestore.doc(`groups/${groupId}/members/${uid}/`);
    const issuerMemberDoc = await issuerMemberRef.get()

    if (issuerMemberDoc?.data()?.role !== "admin") {
        // Throw an HttpsError so that the client gets the error details.
        throw new functions.https.HttpsError('failed-precondition', 'The function must be called from group admins.');
    }

    const groupMemberRef = firestore.doc(`groups/${groupId}/members/${memberId}/`);

    return groupMemberRef.delete().then(_ => groupId);
});

/**
 * Trigger-function - executes when expenses are written to groups.
 * Expenses from a user will also be displayed in their respective user-data.
 * Respective group- and user-balances will also be updated.
 */
export const onGroupExpensesWrite = functions.firestore
    .document('/groups/{groupId}/expenses/{expenseId}')
    .onWrite(async (change, context) => {
        let data      : DocumentData;
        let groupRef  : DocumentReference;
        let groupDoc  : DocumentSnapshot  | undefined;
        let groupData : DocumentData      | undefined;

        let costBefore: number = 0;
        let costAfter : number = 0;

        if (!change.before.exists) {
            // new document created
            data      = change.after.data()!;
            groupRef  = change.after.ref.parent.parent!;
            groupDoc  = await groupRef.get();

            if (!groupDoc.exists) {
                return null;
            }

            groupData = groupDoc?.data();
            costAfter = data.cost;

            await createUserDocIfNotExists(data.userId);

            const userExpense = {
                name: data.name,
                date: data.date,
                groupId: groupRef.id,
                groupName: groupData?.name,
                cost: costAfter
            }

            const userExpenseRef = firestore.doc(`users/${data.userId}/expenses/${change.after.id}`);
            await userExpenseRef.create(userExpense);

        } else if (!change.after.exists) {
            // document was deleted
            data       = change.before.data()!;
            groupRef   = change.before.ref.parent.parent!;
            groupDoc   = await groupRef.get();

            if (!groupDoc.exists) {
                return null;
            }

            groupData  = groupDoc?.data();
            costBefore = data.cost

            const userExpenseRef = firestore.doc(`users/${data?.userId}/expenses/${change.after.id}`);
            await userExpenseRef.delete();
        } else {
            // document was changed
            data       = change.after.data()!;
            groupRef   = change.after.ref.parent.parent!;
            groupDoc   = await groupRef.get();

            if (!groupDoc.exists) {
                return null;
            }

            groupData  = groupDoc?.data();
            costBefore = change.before.data()!.cost;
            costAfter  = data.cost;

            await createUserDocIfNotExists(data.userId);

            const userExpense = {
                name: data?.name,
                date: data?.date,
                groupId: groupRef.id,
                groupName: groupData?.name,
                cost: data?.cost
            }

            const userExpenseRef = firestore.doc(`users/${data.userId}/expenses/${change.after.id}`);
            await userExpenseRef.set(userExpense);
        }

        const groupMemberRef = groupRef.collection(`members`).doc(`${data.userId}`);
        await setGroupMemberExpenses(groupMemberRef, costBefore, costAfter);

        return groupRef.set({
            latestUpdate : new Date(),
            totalExpenses: groupData?.totalExpenses - costBefore + costAfter
        }, {merge: true});
    });

/**
 * Helper function to update group-members expenses by a difference of before and after values.
 *
 * @param groupMemberRef DocumentReference of the respective user-data
 * @param costBefore     value before (0 if payment is created)
 * @param costAfter      value after (0 if payment is deleted)
 *
 * @return promise to update users expenses in group.
 */
async function setGroupMemberExpenses(groupMemberRef: DocumentReference, costBefore: number, costAfter: number) {
    const groupMemberDoc  = await groupMemberRef.get();
    const groupMemberData = groupMemberDoc?.data();

    return groupMemberRef.set({
        totalExpenses: groupMemberData?.totalExpenses - costBefore + costAfter,
    }, {merge: true});
}

/**
 * Helper function to update group-members payments by a difference of before and after values.
 *
 * @param groupMemberRef DocumentReference of the respective user-data
 * @param paymentBefore  value before (0 if payment is created)
 * @param paymentAfter   value after (0 if payment is deleted)
 *
 * @return promise to update users payments in group.
 */
async function setGroupMemberPayments(groupMemberRef: DocumentReference, paymentBefore: number, paymentAfter: number) {
    const groupMemberDoc  = await groupMemberRef.get();
    const groupMemberData = groupMemberDoc?.data();

    return groupMemberRef.set({
        totalPayments: groupMemberData?.totalPayments - paymentBefore + paymentAfter,
    }, {merge: true});
}

/**
 * Trigger-function - executes when payments are written to groups.
 * Payments from a user will also be displayed in their respective user-data.
 * Respective group- and user-balances will also be updated.
 */
export const onGroupPaymentsWrite = functions.firestore
    .document('/groups/{groupId}/payments/{paymentId}')
    .onWrite(async (change, context) => {
        let data      : DocumentData;
        let groupRef  : DocumentReference;
        let groupDoc  : DocumentSnapshot  | undefined;
        let groupData : DocumentData      | undefined;

        let paymentBefore: number = 0;
        let paymentAfter : number = 0;

        if (!change.before.exists) {
            // new document created
            data         = change.after.data()!;
            groupRef     = change.after.ref.parent.parent!;
            groupDoc     = await groupRef.get();

            if (!groupDoc.exists) {
                return null;
            }

            groupData    = groupDoc?.data();
            paymentAfter = data.payment;

            await createUserDocIfNotExists(data?.userId);

            const userPayment = {
                date: data.date,
                groupId: groupRef.id,
                groupName: groupData?.name,
                payment: paymentAfter
            }

            const userPaymentRef = firestore.doc(`users/${data?.userId}/payments/${change.after.id}`);
            await userPaymentRef.create(userPayment);

        } else if (!change.after.exists) {
            // document was deleted
            data          = change.before.data()!;
            groupRef      = change.before.ref.parent.parent!;
            groupDoc      = await groupRef.get();

            if (!groupDoc.exists) {
                return null;
            }

            groupData     = groupDoc?.data();
            paymentBefore = data.payment;

            const userPaymentRef = firestore.doc(`users/${data?.userId}/payments/${change.before.id}`);
            await userPaymentRef.delete();
        } else {
            // document was changed
            data          = change.after.data()!;
            groupRef      = change.after.ref.parent.parent!;
            groupDoc      = await groupRef.get();

            if (!groupDoc.exists) {
                return null;
            }

            groupData     = groupDoc?.data();
            paymentBefore = change.before.data()!.payment;
            paymentAfter  = data.payment;

            await createUserDocIfNotExists(data.userId);

            const userPayment = {
                date: data?.date,
                groupId: groupRef?.id,
                groupName: groupData?.name,
                payment: data?.payment
            }

            const userPaymentRef = firestore.doc(`users/${data?.userId}/payments/${change.after.id}`);
            await userPaymentRef.set(userPayment, {merge: true});
        }

        const groupMemberRef  = groupRef?.collection(`members`).doc(`${data.userId}`);
        await setGroupMemberPayments(groupMemberRef, paymentBefore, paymentAfter);

        return groupRef?.set({
            latestUpdate: new Date(),
            totalPayments: groupData?.totalPayments - paymentBefore + paymentAfter
        }, {merge: true});
    });

/**
 * Trigger-function - executes when expenses are written to user-data.
 * Updates users statistics.
 */
export const onUserExpensesWrite = functions.firestore
    .document('/users/{userId}/expenses/{expenseId}')
    .onWrite(async (change, context) => {
        let costBefore: number = 0;
        let costAfter : number = 0;
        let userRef   : DocumentReference | null;
        let isNew     : boolean = false;

        if (!change.before.exists) {
            // new document created
            isNew     = true;
            costAfter = change.after.data()?.cost;
            userRef   = change.after.ref.parent.parent;
        } else if (!change.after.exists) {
            // document deleted
            costBefore = change.before.data()?.cost;
            userRef    = change.before.ref.parent.parent;
        } else {
            // document changed
            costAfter  = change.after.data()?.cost;
            costBefore = change.before.data()?.cost;
            userRef    = change.after.ref.parent.parent;
        }

        return updateUserTotalExpenses(userRef!, costBefore, costAfter, isNew);
    });

/**
 * Trigger-function - executes when users join groups.
 * Updates the number of groups in the respective user-data.
 */
export const onUserGroupsCreate = functions.firestore
    .document('users/{userId}/groups/{groupId}')
    .onCreate(async (snap, _) => {
        const userRef = snap.ref.parent.parent!;
        const userDoc = await userRef.get();

        const numGroups = userDoc!.data()!.numGroups;
        return userRef.set({ numGroups: numGroups + 1 }, { merge: true });
    });

/**
 * Trigger-function - executes when users leave groups.
 * Updates the number of groups in the respective user-data.
 */
export const onUserGroupsDelete = functions.firestore
    .document('users/{userId}/groups/{groupId}')
    .onDelete(async (snap, _) => {
        const userRef = snap.ref.parent.parent!;
        const userDoc = await userRef.get();

        if (!userDoc) {
            return;
        }

        const numGroups = userDoc!.data()!.numGroups;
        return userRef.set({ numGroups: numGroups - 1 }, { merge: true });
    });

/**
 * Helper function to update users total expenses by a difference of before and after values.
 *
 * @param userRef    DocumentReference of the respective user-data
 * @param costBefore value before (0 if payment is created)
 * @param costAfter  value after (0 if payment is deleted)
 * @param isNew      flag if the payment is the users first payment of all time.
 *
 * @return promise to update users total expenses in user-data.
 */
async function updateUserTotalExpenses(userRef   : DocumentReference,
                                       costBefore: number,
                                       costAfter : number,
                                       isNew     : boolean) {
    const userDoc  = await userRef.get();
    const userData = userDoc?.data();
    let   oldAchievementProgress = userData?.achievementProgress;

    if (oldAchievementProgress == undefined) {
        oldAchievementProgress = {
            expensesCount     : 0,
            paymentsCount     : 0,
            maxNegativeBalance: 0
        };
    }

    const expensesCount = isNew ? oldAchievementProgress.expensesCount + 1 : oldAchievementProgress.expensesCount;

    return userRef.set({
        latestUpdate: new Date(),
        totalExpenses: userData?.totalExpenses - costBefore + costAfter,
        achievementProgress: {
            expensesCount     : expensesCount,
            paymentsCount     : oldAchievementProgress.paymentsCount,
            maxNegativeBalance: oldAchievementProgress.maxNegativeBalance
        }
    }, {merge: true});
}

/**
 * Trigger-function - executes when payments are written to user-data.
 * Updates the respective users statistic.
 */
export const onUserPaymentWrite = functions.firestore
    .document('/users/{userId}/payments/{paymentId}')
    .onWrite(async (change, context) => {
        let paymentBefore: number = 0;
        let paymentAfter : number = 0;
        let userRef      : DocumentReference | null;
        let isNew        : boolean = false;

        if (!change.before.exists) {
            // new document created
            isNew        = true;
            paymentAfter = change.after.data()?.payment;
            userRef      = change.after.ref.parent.parent;
        } else if (!change.after.exists) {
            // document deleted
            paymentBefore = change.before.data()?.payment;
            userRef       = change.before.ref.parent.parent;
        } else {
            // document changed
            paymentAfter  = change.after.data()?.payment;
            paymentBefore = change.before.data()?.payment;
            userRef       = change.after.ref.parent.parent;
        }

        return updateUserTotalPayments(userRef!, paymentBefore, paymentAfter, isNew);
    });

/**
 * Trigger-function - executes when member documents are written to groups.
 * Updates statistics of the respective group.
 */
export const onGroupMembersWrite = functions.firestore
    .document('/groups/{groupId}/members/{memberId}')
    .onWrite(async (change, _) => {
        if (!change.after.exists) {
            // document deleted
            const groupRef     = change.before.ref.parent.parent!;
            const groupId      = groupRef.id;
            const userId       = change.before.id;
            const userGroupRef = firestore.doc(`/users/${userId}/groups/${groupId}`);
            return userGroupRef.delete();
        } else {
            // document changed
            const data      = change.after.data()!;
            const groupRef  = change.after.ref.parent.parent!;
            const groupDoc  = await groupRef.get();

            if (!groupDoc.exists) {
                return null;
            }

            const groupData = groupDoc?.data();
            const groupId   = groupRef.id;
            const groupName = groupData?.name;

            const userId       = change.after.id;
            const userGroupRef = firestore.doc(`/users/${userId}/groups/${groupId}`);

            return userGroupRef.set({
                name            : groupName,
                personalExpenses: data.totalExpenses,
                personalPayments: data.totalPayments
            }, {merge: true});
        }
    });

/**
 * Helper function to update users total payments by a difference of before and after values.
 *
 * @param userRef       DocumentReference of the respective user-data
 * @param paymentBefore value before (0 if payment is created)
 * @param paymentAfter  value after (0 if payment is deleted)
 * @param isNew         flag if the payment is the users first payment of all time.
 *
 * @return promise to update users total payments in user-data.
 */
async function updateUserTotalPayments(userRef      : DocumentReference,
                                       paymentBefore: number,
                                       paymentAfter : number,
                                       isNew        : boolean) {
    const userDoc  = await userRef.get();
    const userData = userDoc?.data();
    let   oldAchievementProgress = userData?.achievementProgress;

    if (oldAchievementProgress == undefined) {
        oldAchievementProgress = {
            expensesCount : 0,
            paymentsCount : 0,
            maxNegativeBalance: 0
        };
    }

    const paymentsCount = isNew ? oldAchievementProgress.paymentsCount + 1 : oldAchievementProgress.paymentsCount;

    return userRef.set({
        latestUpdate: new Date(),
        totalPayments: userData?.totalPayments - paymentBefore + paymentAfter,
        achievementProgress: {
            expensesCount : oldAchievementProgress.expensesCount,
            paymentsCount : paymentsCount,
            maxNegativeBalance: oldAchievementProgress.maxNegativeBalance
        }
    }, {merge: true});
}

/**
 * Creates user-data if not exists.
 *
 * @param uid id of the user the user-data is mapped to
 *
 * @return promise to create respective user-data or null if the user-data already exists
 */
async function createUserDocIfNotExists(uid: string) {
    const userRef = firestore.doc(`users/${uid}`);
    const userDoc = await userRef.get();

    if (!userDoc.exists) {
        const userData = {
            totalExpenses: 0,
            totalPayments: 0,
            numGroups    : 0,
            achievementProgress: {
                expensesCount     : 0,
                paymentsCount     : 0,
                maxNegativeBalance: 0
            }
        };

        return userRef.create(userData);
    }
    return null;
}

/**
 * Trigger-function - executes when groups are updated.
 * Updates statistics of the respective group.
 */
export const onGroupUpdate = functions.firestore
    .document('groups/{groupId}')
    .onUpdate(async (change, _) => {
        const groupId = change.after.id;
        const groupData = change.after.data();

        const membersSnap = await change.after.ref.collection('members').get()

        const fetchList = membersSnap.docs.map(snap => {
            firestore.doc(`users/${snap.id}/groups/${groupId}`).set({
                totalExpenses: groupData.totalExpenses,
                totalPayments: groupData.totalPayments,
                numMembers   : groupData.numMembers,
                latestUpdate : groupData.latestUpdate
            }, {merge: true})
        })

        return Promise.all(fetchList);
    });

/**
 * Trigger-function - executes when groups are deleted.
 * Removes entries from user-data if users were members, when a group was deleted.
 */
export const onGroupDelete = functions.firestore
    .document('/groups/{groupId}')
    .onDelete(async snapshot => {
        const members = await snapshot.ref.collection('members').listDocuments()
        const fetchList = members.map(ref => ref.delete());
        return Promise.all(fetchList);
    });

/**
 * Trigger-function - executes when member documents are created in groups.
 * Updates statistics of the respective group.
 */
export const onGroupMemberCreate = functions.firestore
    .document('groups/{groupId}/members/{memberId}')
    .onCreate(async (snap, _) => {
        const groupRef  = snap.ref.parent.parent!;
        const groupDoc  = await groupRef.get();

        if (!groupDoc.exists) {
            return null;
        }

        const groupData = groupDoc?.data();

        return groupRef.set({
            latestUpdate: new Date(),
            numMembers  : groupData?.numMembers + 1
        }, {merge: true});
    });

/**
 * Trigger-function - executes when member documents are deleted in groups.
 * Updates statistics of the respective group.
 */
export const onGroupMemberDelete = functions.firestore
    .document('groups/{groupId}/members/{memberId}')
    .onDelete(async (snap) => {
        const groupRef  = snap.ref.parent.parent!;
        const groupDoc  = await groupRef.get();

        if (!groupDoc.exists) {
            return null;
        }

        const groupData = groupDoc?.data();

        return groupRef.set({
            latestUpdate: new Date(),
            numMembers  : groupData?.numMembers - 1
        }, {merge: true});
    })

/**
 * Trigger-function - executes when users register to the app.
 * Respective user-data will be created
 */
export const onUserCreate = functions.auth.user()
    .onCreate(async user => createUserDocIfNotExists(user.uid));

/**
 * Trigger-function - executes when users unregister from the app.
 * Respective user-data will be deleted
 */
export const onUserDelete = functions.auth.user()
    .onDelete(async user => firestore.doc(`users/${user.uid}`).delete());


// *************************
// ********* Jobs **********
// *************************

// Job: Check recurring costs for group expenses, check negative balance for users

/**
 * Worker routine checking database for recurring expenses.
 * Expenses marked as recurring, will be added to the total expenses in their respective intervals.
 */
export const checkRecurringAndBalanceJob = functions.pubsub.schedule('every 5 minutes') // at 00:00 would be '0 0 * * *'
    .timeZone('Europe/Berlin')
    .onRun(async (ctx) => checkRecurringAndBalance());


/**
 * Functionality to check recurring expenses
 */
async function checkRecurringAndBalance() {
    log("Running check recurring and balance job...");

    const groupsCollectionRef = firestore.collection("groups");
    const groupsSnapshot      = await groupsCollectionRef.get();

    groupsSnapshot.forEach((groupDoc) => {
        checkRecurringForGroup(groupDoc);
    });

    const usersCollectionRef = firestore.collection("users");
    const usersSnapshot      = await usersCollectionRef.get();

    usersSnapshot.forEach((userDoc) => {
        checkBalanceForUser(userDoc);
    });

    log("Done!");
}


/**
 * Checks for recurring expenses in the specified group and renews expenses that apply.
 *
 * @param group The group to be checked
 */
async function checkRecurringForGroup(group: FirebaseFirestore.QueryDocumentSnapshot<FirebaseFirestore.DocumentData>) {
    const expensesRef      = group.ref.collection("expenses");
    const expensesSnapshot = await expensesRef.get();

    const groupName = group.data()?.name;

    log(`group: ${groupName}`);

    if (groupName === undefined) {
        log(`invalid data for group ${group.id}. Group name is undefined.`);
    }

    if (expensesSnapshot === null) {
        return;
    }

    log(util.inspect(expensesSnapshot.docs));

    for (const expenseDoc of expensesSnapshot.docs) {
        log("expenseDoc: " + util.inspect(expenseDoc));
        const expenseData = expenseDoc.data();
        const expenseDate = expenseData?.date?.toDate();
        const now         = new Date();
        const days        = dayOffset(expenseDate, now)
        const isRecurring = expenseData?.recurring;
        log(`name: ${expenseData?.name}, dayOffset: ${days}, recurring: ${isRecurring}, alreadyRecurred: 
        ${expenseData?.alreadyRecurred}, -> ${isRecurring && days < 1 && expenseData?.alreadyRecurred !== true}`);

        if (isRecurring && days < 1) {
            if (expenseData?.alreadyRecurred === true) {
                continue;
            }

            // log("Recurring expense found!");

            // add new expense for recurring cost
            const intervalType: number  = expenseData?.recurringInterval >> 29;
            const intervalValue: number = expenseData?.recurringInterval & intervalValueMask;

            // log(`type: ${intervalType}`);
            // log(`value: ${intervalValue}`);

            const newDate = new Date(expenseDate);

            switch (intervalType) {
                case 0: // Day
                    addDays(newDate, intervalValue);
                    break;
                case 1: // Week
                    addDays(newDate, 7 * intervalValue);
                    break;
                case 2: // Month
                    for (let i = 0; i < intervalValue; i++) {
                        addDays(newDate, daysInMonth(newDate.getMonth(), newDate.getFullYear()));
                    }
                    break;
                case 3: // Year
                    newDate.setFullYear(newDate.getFullYear() + intervalValue);
                    break;
            }

            const newExpenseData = {
                name: expenseData.name,
                cost: expenseData.cost,
                date: newDate,
                recurring: true,
                alreadyRecurred: false,
                recurringInterval: expenseData.recurringInterval,
                userId: expenseData.userId,
                userName: expenseData.userName,
            };

            log("Creating new recurring expense...");
            await expensesRef.doc().create(newExpenseData);

            log("Updating recurring expense...");
            expenseDoc.ref?.set({alreadyRecurred: true}, {merge: true}).then(_ => log(`Done.`), _ => log(`failed`));
        } else {
            // log(`Expense skipped: ${isRecurring}, ${days}`);
        }
    }

    log(`Done! No new documents found for group ${groupName}.`);
}


/**
 * Checks and updates the maximum negative balance for the specified user's userDoc.
 *
 * @param userDoc The userDoc of the user to be checked
 */
async function checkBalanceForUser(userDoc: FirebaseFirestore.QueryDocumentSnapshot<FirebaseFirestore.DocumentData>) {
    // log(`user: ${userDoc.id}`);

    const userData = userDoc.data();

    let oldAchievementProgress = userData?.achievementProgress;

    if (oldAchievementProgress == undefined) {
        oldAchievementProgress = {
            expensesCount     : 0,
            paymentsCount     : 0,
            maxNegativeBalance: 0
        };
    }

    let userBalance = 0;

    const groupsCollectionRef = firestore.collection(`users/${userDoc.id}/groups`);
    groupsCollectionRef.get().then((groupsSnapshot) => {
        groupsSnapshot.forEach(async (groupDoc) => {
            const groupData = groupDoc.data();

            const numMembers       = groupData?.numMembers;
            const totalExpenses    = groupData?.totalExpenses;
            const personalPayments = groupData?.personalPayments;

            // log(`groupId: ${groupDoc.id}`);
            // log(`numMembers: ${numMembers}`);
            // log(`totalExpenses: ${totalExpenses}`);
            // log(`personalPayments: ${personalPayments}`);

            const personalBalance = personalPayments - (totalExpenses / numMembers);

            userBalance += personalBalance;

            // log(`userBalance: ${userBalance}`);
            // log(`maxNegativeBalance: ${oldAchievementProgress.maxNegativeBalance}`);

            if (userBalance < oldAchievementProgress.maxNegativeBalance) {
                // log(`updating...`);
                // update maxNegativeBalance with the userBalance
                await userDoc.ref.set({
                    latestUpdate: new Date(),
                    totalExpenses: userData?.totalExpenses,
                    achievementProgress: {
                        expensesCount: oldAchievementProgress.expensesCount,
                        paymentsCount: oldAchievementProgress.paymentsCount,
                        maxNegativeBalance: userBalance
                    }
                }, {merge: true});
            }
        })
    });
}


/**
 * Returns the number of days in the specified month for the specified year.
 *
 * @param month The month which day count should be determined
 * @param year  The year for which the day count of the specified month should be determined
 *
 * @return the number of days in the specified month for the specified year.
 *
 * @see https://stackoverflow.com/questions/1184334/get-number-days-in-a-specified-month-using-javascript
 */
function daysInMonth(month: number, year: number): number {
    return new Date(year, month + 1, 0).getDate();
}


/**
 * Adds the specified amount of days to the provided date.
 *
 * @param date  The date to which the days should be added.
 * @param count The number of days to be added.
 */
function addDays(date: Date, count: number): void {
    date.setTime(date.getTime() + count * (1000 * 60 * 60 * 24));
}

/**
 * This function tests the day offset between two days.
 * @param date1 first date
 * @param date2 second date, e.g. the current date
 * @return positive values means that the second date is past the first date and vice versa.
 */
function dayOffset(date1: Date, date2: Date): number {
    return (date1.getTime() - date2.getTime()) / (1000 * 60 * 60 * 24)
}
