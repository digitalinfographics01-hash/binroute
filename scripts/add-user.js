/**
 * Add a user to BinRoute.
 *
 * Usage: node scripts/add-user.js <username> <password>
 */
const bcrypt = require('bcryptjs');
const { initializeDatabase } = require('../src/db/schema');
const { querySql, runSql, closeDb } = require('../src/db/connection');

(async () => {
  const username = process.argv[2];
  const password = process.argv[3];

  if (!username || !password) {
    console.log('Usage: node scripts/add-user.js <username> <password>');
    process.exit(1);
  }

  await initializeDatabase();

  const existing = querySql('SELECT id FROM users WHERE username = ?', [username])[0];
  if (existing) {
    console.log(`User "${username}" already exists.`);
    closeDb();
    process.exit(1);
  }

  const hash = bcrypt.hashSync(password, 10);
  runSql('INSERT INTO users (username, password_hash) VALUES (?, ?)', [username, hash]);
  console.log(`User "${username}" created.`);
  closeDb();
})();
