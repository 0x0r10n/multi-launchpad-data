import sqlite3 from 'sqlite3';

const db = new sqlite3.Database('./mainnet.db');

db.all("PRAGMA table_info(tokens);", (err, columns) => {
    console.log("Columns:", columns);
    db.get("SELECT * FROM tokens WHERE bonding_curve IS NOT NULL LIMIT 1;", (err, row) => {
        console.log("Row with bonding_curve:", row);
        db.get("SELECT * FROM tokens LIMIT 1;", (err, row) => {
            console.log("Sample Row:", row);
            db.close();
        });
    });
});
