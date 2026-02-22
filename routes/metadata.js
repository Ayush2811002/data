const express = require("express");
const router = express.Router();
const { createConnection } = require("../db/connection");
const axios = require("axios");

// 🔗 Python AI service (Render or local fallback)
const PYTHON_SERVICE_URL =
  process.env.PYTHON_SERVICE_URL || "http://127.0.0.1:8000";

console.log("🔥 PYTHON_SERVICE_URL =", PYTHON_SERVICE_URL);
if (!PYTHON_SERVICE_URL) {
  console.warn("⚠️ PYTHON_SERVICE_URL not set");
}
// router.post("/extract", async (req, res) => {
//   console.log("📩 /api/metadata/extract called");

//   const config = req.body;

//   try {
//     // ==========================
//     // ✅ DATABASE CONNECTION
//     // ==========================
//     const connection = await createConnection(config);
//     console.log("✅ Database connected");

//     // ==========================
//     // ✅ FETCH TABLES
//     // ==========================
//     const [tables] = await connection.execute(
//       `
//       SELECT table_name
//       FROM information_schema.tables
//       WHERE table_schema = ?
//       `,
//       [config.database],
//     );

//     console.log(`📊 Tables found: ${tables.length}`);

//     const metadata = [];

//     // ==========================
//     // 🔁 LOOP TABLES
//     // ==========================
//     for (let table of tables) {
//       const tableName = table.TABLE_NAME || table.table_name;
//       console.log(`\n📄 Processing table: ${tableName}`);

//       // --------------------------
//       // COLUMNS
//       // --------------------------
//       const [columns] = await connection.execute(
//         `
//         SELECT column_name, data_type
//         FROM information_schema.columns
//         WHERE table_schema = ?
//         AND table_name = ?
//         `,
//         [config.database, tableName],
//       );

//       // --------------------------
//       // PRIMARY KEYS
//       // --------------------------
//       const [primaryKeys] = await connection.execute(
//         `
//         SELECT column_name
//         FROM information_schema.key_column_usage
//         WHERE table_schema = ?
//         AND table_name = ?
//         AND constraint_name = 'PRIMARY'
//         `,
//         [config.database, tableName],
//       );

//       // --------------------------
//       // FOREIGN KEYS
//       // --------------------------
//       const [foreignKeys] = await connection.execute(
//         `
//         SELECT
//           column_name,
//           referenced_table_name,
//           referenced_column_name
//         FROM information_schema.key_column_usage
//         WHERE table_schema = ?
//         AND table_name = ?
//         AND referenced_table_name IS NOT NULL
//         `,
//         [config.database, tableName],
//       );

//       // --------------------------
//       // ENRICH COLUMNS
//       // --------------------------
//       const enrichedColumns = columns.map((col) => {
//         const isPK = primaryKeys.some(
//           (pk) => pk.column_name === col.column_name,
//         );
//         const isFK = foreignKeys.some(
//           (fk) => fk.column_name === col.column_name,
//         );

//         return {
//           name: col.column_name,
//           type: col.data_type,
//           isPrimaryKey: isPK,
//           isForeignKey: isFK,
//         };
//       });

//       const relationships = foreignKeys.map((fk) => ({
//         column: fk.column_name,
//         references: `${fk.referenced_table_name}.${fk.referenced_column_name}`,
//       }));

//       // ==========================
//       // 🤖 AI BUSINESS SUMMARY
//       // ==========================
//       console.log(`🤖 Calling AI summary for table: ${tableName}`);
//       console.log("➡️ URL:", `${PYTHON_SERVICE_URL}/generate-summary`);

//       const aiResponse = await axios.post(
//         `${PYTHON_SERVICE_URL}/generate-summary`,
//         {
//           tableName,
//           columns: enrichedColumns,
//         },
//       );

//       console.log("✅ AI summary received");

//       // ==========================
//       // 📊 FETCH TABLE ROWS
//       // ==========================
//       // ✅ TOTAL ROW COUNT
//       const [[{ rowCount }]] = await connection.execute(
//         `SELECT COUNT(*) AS rowCount FROM \`${tableName}\``,
//       );
//       const [rows] = await connection.execute(
//         `SELECT * FROM \`${tableName}\` LIMIT 1000`,
//       );

//       // ==========================
//       // 📊 DATA QUALITY ANALYSIS
//       // ==========================
//       console.log(`📊 Analyzing data quality for ${tableName}`);
//       console.log("➡️ URL:", `${PYTHON_SERVICE_URL}/analyze-data`);

//       const qualityResponse = await axios.post(
//         `${PYTHON_SERVICE_URL}/analyze-data`,
//         {
//           tableName,
//           rows,
//         },
//       );

//       console.log("✅ Data quality received");
//       const metrics = qualityResponse.data.metrics || [];

//       /* ✅ COMPLETENESS SCORE */
//       const completenessScore =
//         metrics.reduce((acc, col) => acc + (col.completeness || 0), 0) /
//         (metrics.length || 1);

//       /* ✅ UNIQUENESS SCORE */
//       const uniquenessScore =
//         metrics.reduce((acc, col) => acc + (col.uniqueness || 0), 0) /
//         (metrics.length || 1);

//       /* ✅ FRESHNESS SCORE (time decay) */
//       const lastUpdatedTime = new Date(
//         qualityResponse.data.freshness?.lastUpdated,
//       ).getTime();

//       const hoursAgo = (Date.now() - lastUpdatedTime) / (1000 * 60 * 60);

//       let freshnessScore = 0;

//       if (hoursAgo <= 1) freshnessScore = 100;
//       else if (hoursAgo <= 6) freshnessScore = 95;
//       else if (hoursAgo <= 12) freshnessScore = 90;
//       else if (hoursAgo <= 24)
//         freshnessScore = 80; // 👈 YOUR TARGET
//       else if (hoursAgo <= 48) freshnessScore = 65;
//       else if (hoursAgo <= 72) freshnessScore = 50;
//       else freshnessScore = 30;
//       // ==========================
//       // 📦 PUSH FINAL TABLE METADATA
//       // ==========================
//       metadata.push({
//         tableName,
//         rowCount, // 🔥 THIS FIXES YOUR UI

//         /* ✅ TABLE LEVEL KPIs */
//         completeness: { score: completenessScore },
//         uniqueness: { score: uniquenessScore },
//         freshness: {
//           ...qualityResponse.data.freshness,
//           score: freshnessScore,
//         },
//         displayMetrics: {
//           rowsLabel: rowCount.toLocaleString(),
//           columnsLabel: enrichedColumns.length,
//         },
//         businessSummary: aiResponse.data.businessSummary,
//         columns: enrichedColumns,
//         relationships,
//         dataQuality: qualityResponse.data.metrics,

//         risks: qualityResponse.data.risks,
//       });
//     }

//     console.log("🚀 Metadata extraction completed");
//     res.json(metadata);
//   } catch (error) {
//     console.error("❌ ERROR:", error.message);
//     res.status(500).json({ error: error.message });
//   }
// });

router.post("/extract", async (req, res) => {
  console.log("📩 /api/metadata/extract called");

  const config = req.body;

  // ✅ INPUT VALIDATION
  if (!config?.host || !config?.user || !config?.database) {
    console.error("❌ Invalid DB config:", config);

    return res.status(400).json({
      error: "Invalid database configuration",
      details: "host, user, database required",
    });
  }

  let connection;

  try {
    // ==========================
    // ✅ DATABASE CONNECTION
    // ==========================
    try {
      connection = await createConnection(config);
      console.log("✅ Database connected");
    } catch (dbError) {
      console.error("❌ DATABASE CONNECTION FAILED");
      console.error(dbError);

      return res.status(500).json({
        error: "Database connection failed",
        details: dbError.message,
      });
    }

    // ==========================
    // ✅ FETCH TABLES
    // ==========================
    let tables;

    try {
      const [result] = await connection.execute(
        `
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = ?
        `,
        [config.database],
      );

      tables = result;
      console.log(`📊 Tables found: ${tables.length}`);
    } catch (tablesError) {
      console.error("❌ FAILED FETCHING TABLES");
      console.error(tablesError);

      return res.status(500).json({
        error: "Failed fetching tables",
        details: tablesError.message,
      });
    }

    const metadata = [];

    for (let table of tables) {
      const tableName = table.TABLE_NAME || table.table_name;

      console.log(`📄 Processing table: ${tableName}`);

      let columns, primaryKeys, foreignKeys;

      try {
        [columns] = await connection.execute(
          `
          SELECT column_name, data_type
          FROM information_schema.columns
          WHERE table_schema = ?
          AND table_name = ?
          `,
          [config.database, tableName],
        );

        [primaryKeys] = await connection.execute(
          `
          SELECT column_name
          FROM information_schema.key_column_usage
          WHERE table_schema = ?
          AND table_name = ?
          AND constraint_name = 'PRIMARY'
          `,
          [config.database, tableName],
        );

        [foreignKeys] = await connection.execute(
          `
          SELECT column_name, referenced_table_name, referenced_column_name
          FROM information_schema.key_column_usage
          WHERE table_schema = ?
          AND table_name = ?
          AND referenced_table_name IS NOT NULL
          `,
          [config.database, tableName],
        );
      } catch (schemaError) {
        console.error(`❌ SCHEMA FETCH FAILED → ${tableName}`);
        console.error(schemaError);

        return res.status(500).json({
          error: `Schema fetch failed for ${tableName}`,
          details: schemaError.message,
        });
      }

      const enrichedColumns = columns.map((col) => {
        const isPK = primaryKeys.some(
          (pk) => pk.column_name === col.column_name,
        );

        const isFK = foreignKeys.some(
          (fk) => fk.column_name === col.column_name,
        );

        return {
          name: col.column_name,
          type: col.data_type,
          isPrimaryKey: isPK,
          isForeignKey: isFK,
        };
      });

      const relationships = foreignKeys.map((fk) => ({
        column: fk.column_name,
        references: `${fk.referenced_table_name}.${fk.referenced_column_name}`,
      }));

      // ==========================
      // 🤖 AI SUMMARY
      // ==========================
      let aiResponse;

      try {
        console.log(`🤖 AI Summary → ${tableName}`);

        aiResponse = await axios.post(
          `${PYTHON_SERVICE_URL}/generate-summary`,
          { tableName, columns: enrichedColumns },
          { timeout: 15000 }, // ✅ IMPORTANT
        );

        console.log("✅ AI summary received");
      } catch (aiError) {
        console.error("❌ PYTHON SUMMARY FAILED");
        console.error(aiError.response?.data || aiError.message);

        return res.status(500).json({
          error: "Python summary service failed",
          details: aiError.message,
        });
      }

      // ==========================
      // 📊 TABLE DATA
      // ==========================
      let rowCount, rows;

      try {
        [[{ rowCount }]] = await connection.execute(
          `SELECT COUNT(*) AS rowCount FROM \`${tableName}\``,
        );

        [rows] = await connection.execute(
          `SELECT * FROM \`${tableName}\` LIMIT 1000`,
        );
      } catch (queryError) {
        console.error(`❌ TABLE QUERY FAILED → ${tableName}`);
        console.error(queryError);

        return res.status(500).json({
          error: `Query failed for ${tableName}`,
          details: queryError.message,
        });
      }

      // ==========================
      // 📊 DATA QUALITY
      // ==========================
      let qualityResponse;

      try {
        console.log(`📊 Quality Analysis → ${tableName}`);

        qualityResponse = await axios.post(
          `${PYTHON_SERVICE_URL}/analyze-data`,
          { tableName, rows },
          { timeout: 20000 }, // ✅ IMPORTANT
        );

        console.log("✅ Data quality received");
      } catch (qualityError) {
        console.error("❌ PYTHON QUALITY FAILED");
        console.error(qualityError.response?.data || qualityError.message);

        return res.status(500).json({
          error: "Python quality service failed",
          details: qualityError.message,
        });
      }

      const metrics = qualityResponse.data.metrics || [];

      const completenessScore =
        metrics.reduce((acc, col) => acc + (col.completeness || 0), 0) /
        (metrics.length || 1);

      const uniquenessScore =
        metrics.reduce((acc, col) => acc + (col.uniqueness || 0), 0) /
        (metrics.length || 1);

      metadata.push({
        tableName,
        rowCount,
        completeness: { score: completenessScore },
        uniqueness: { score: uniquenessScore },
        businessSummary: aiResponse.data.businessSummary,
        columns: enrichedColumns,
        relationships,
        dataQuality: metrics,
        risks: qualityResponse.data.risks,
      });
    }

    console.log("🚀 Metadata extraction completed");

    res.json(metadata);
  } catch (error) {
    console.error("❌ UNEXPECTED SERVER ERROR");
    console.error(error);

    res.status(500).json({
      error: "Unexpected server error",
      details: error.message,
    });
  } finally {
    if (connection) {
      await connection.end();
      console.log("🔌 DB connection closed");
    }
  }
});

module.exports = router;
