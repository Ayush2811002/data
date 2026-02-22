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
router.post("/extract", async (req, res) => {
  console.log("📩 /api/metadata/extract called");

  const config = req.body;

  try {
    // ==========================
    // ✅ DATABASE CONNECTION
    // ==========================
    const connection = await createConnection(config);
    console.log("✅ Database connected");

    // ==========================
    // ✅ FETCH TABLES
    // ==========================
    const [tables] = await connection.execute(
      `
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = ?
      `,
      [config.database],
    );

    console.log(`📊 Tables found: ${tables.length}`);

    const metadata = [];

    // ==========================
    // 🔁 LOOP TABLES
    // ==========================
    for (let table of tables) {
      const tableName = table.TABLE_NAME || table.table_name;
      console.log(`\n📄 Processing table: ${tableName}`);

      // --------------------------
      // COLUMNS
      // --------------------------
      const [columns] = await connection.execute(
        `
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = ?
        AND table_name = ?
        `,
        [config.database, tableName],
      );

      // --------------------------
      // PRIMARY KEYS
      // --------------------------
      const [primaryKeys] = await connection.execute(
        `
        SELECT column_name
        FROM information_schema.key_column_usage
        WHERE table_schema = ?
        AND table_name = ?
        AND constraint_name = 'PRIMARY'
        `,
        [config.database, tableName],
      );

      // --------------------------
      // FOREIGN KEYS
      // --------------------------
      const [foreignKeys] = await connection.execute(
        `
        SELECT
          column_name,
          referenced_table_name,
          referenced_column_name
        FROM information_schema.key_column_usage
        WHERE table_schema = ?
        AND table_name = ?
        AND referenced_table_name IS NOT NULL
        `,
        [config.database, tableName],
      );

      // --------------------------
      // ENRICH COLUMNS
      // --------------------------
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
      // 🤖 AI BUSINESS SUMMARY
      // ==========================
      console.log(`🤖 Calling AI summary for table: ${tableName}`);
      console.log("➡️ URL:", `${PYTHON_SERVICE_URL}/generate-summary`);

      const aiResponse = await axios.post(
        `${PYTHON_SERVICE_URL}/generate-summary`,
        {
          tableName,
          columns: enrichedColumns,
        },
      );

      console.log("✅ AI summary received");

      // ==========================
      // 📊 FETCH TABLE ROWS
      // ==========================
      // ✅ TOTAL ROW COUNT
      const [[{ rowCount }]] = await connection.execute(
        `SELECT COUNT(*) AS rowCount FROM \`${tableName}\``,
      );
      const [rows] = await connection.execute(
        `SELECT * FROM \`${tableName}\` LIMIT 1000`,
      );

      // ==========================
      // 📊 DATA QUALITY ANALYSIS
      // ==========================
      console.log(`📊 Analyzing data quality for ${tableName}`);
      console.log("➡️ URL:", `${PYTHON_SERVICE_URL}/analyze-data`);

      const qualityResponse = await axios.post(
        `${PYTHON_SERVICE_URL}/analyze-data`,
        {
          tableName,
          rows,
        },
      );

      console.log("✅ Data quality received");
      const metrics = qualityResponse.data.metrics || [];

      /* ✅ COMPLETENESS SCORE */
      const completenessScore =
        metrics.reduce((acc, col) => acc + (col.completeness || 0), 0) /
        (metrics.length || 1);

      /* ✅ UNIQUENESS SCORE */
      const uniquenessScore =
        metrics.reduce((acc, col) => acc + (col.uniqueness || 0), 0) /
        (metrics.length || 1);

      /* ✅ FRESHNESS SCORE (time decay) */
      const lastUpdatedTime = new Date(
        qualityResponse.data.freshness?.lastUpdated,
      ).getTime();

      const hoursAgo = (Date.now() - lastUpdatedTime) / (1000 * 60 * 60);

      let freshnessScore = 0;

      if (hoursAgo <= 1) freshnessScore = 100;
      else if (hoursAgo <= 6) freshnessScore = 95;
      else if (hoursAgo <= 12) freshnessScore = 90;
      else if (hoursAgo <= 24)
        freshnessScore = 80; // 👈 YOUR TARGET
      else if (hoursAgo <= 48) freshnessScore = 65;
      else if (hoursAgo <= 72) freshnessScore = 50;
      else freshnessScore = 30;
      // ==========================
      // 📦 PUSH FINAL TABLE METADATA
      // ==========================
      metadata.push({
        tableName,
        rowCount, // 🔥 THIS FIXES YOUR UI

        /* ✅ TABLE LEVEL KPIs */
        completeness: { score: completenessScore },
        uniqueness: { score: uniquenessScore },
        freshness: {
          ...qualityResponse.data.freshness,
          score: freshnessScore,
        },
        displayMetrics: {
          rowsLabel: rowCount.toLocaleString(),
          columnsLabel: enrichedColumns.length,
        },
        businessSummary: aiResponse.data.businessSummary,
        columns: enrichedColumns,
        relationships,
        dataQuality: qualityResponse.data.metrics,

        risks: qualityResponse.data.risks,
      });
    }

    console.log("🚀 Metadata extraction completed");
    res.json(metadata);
  } catch (error) {
    console.error("❌ ERROR:", error.message);
    res.status(500).json({ error: error.message });
  }
});

module.exports = router;
