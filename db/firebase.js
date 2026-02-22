const admin = require("firebase-admin");

function initFirebase(serviceAccount, databaseURL) {
  if (!admin.apps.length) {
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
      databaseURL,
    });
  }

  return admin.database();
}

module.exports = { initFirebase };
