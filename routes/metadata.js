const express = require("express");
const router = express.Router();
const { createConnection } = require("../db/connection");
// ⚠ IMPORTANT FIX → "../db/connection" (not "./db/connection")
const axios = require("axios");

router.post("/extract", async (req, res) => {
  const config = req.body;

  try {
    const connection = await createConnection(config);

    // ✅ Fetch Tables
    const [tables] = await connection.execute(
      `
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = ?
    `,
      [config.database],
    );

    const metadata = [];

    for (let table of tables) {
      const tableName = table.TABLE_NAME || table.table_name;

      // ✅ Fetch Columns
      const [columns] = await connection.execute(
        `
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = ? 
        AND table_name = ?
      `,
        [config.database, tableName],
      );

      // ✅ Fetch Primary Keys
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

      // ✅ Fetch Foreign Keys
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

      //   metadata.push({
      //     table: tableName,
      //     columns: columns,
      //     primaryKeys: primaryKeys,
      //     foreignKeys: foreignKeys,
      //   });

      //   const enrichedColumns = columns.map((col) => {
      //     const isPK = primaryKeys.some(
      //       (pk) => pk.column_name === col.column_name,
      //     );
      //     const isFK = foreignKeys.some(
      //       (fk) => fk.column_name === col.column_name,
      //     );

      //     return {
      //       name: col.column_name,
      //       type: col.data_type,
      //       isPrimaryKey: isPK,
      //       isForeignKey: isFK,
      //     };
      //   });
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

      // ✅ CALL PYTHON AI SERVICE 🔥
      const aiResponse = await axios.post(
        "http://127.0.0.1:8000/generate-summary",
        {
          tableName: tableName,
          columns: enrichedColumns,
        },
      );

      metadata.push({
        tableName: tableName,
        businessSummary: aiResponse.data.businessSummary, // 🤖 FROM PYTHON
        columns: enrichedColumns,
        relationships: relationships,
      });
    }

    // res.json(metadata);
    // ✅ CALL PYTHON DATA QUALITY ENGINE
    const qualityResponse = await axios.post(
      "http://localhost:8000/analyze-data",
      {
        host: config.host,
        user: config.user,
        password: config.password,
        database: config.database,
        tables: metadata.map((table) => ({
          tableName: table.tableName,
        })),
      },
    );

    // ✅ MERGE QUALITY DATA
    const qualityMetrics = qualityResponse.data;

    const finalMetadata = metadata.map((table) => {
      const metrics = qualityMetrics.find(
        (m) => m.tableName === table.tableName,
      );

      return {
        ...table,
        dataQuality: metrics ? metrics.metrics : [],
        freshness: metrics ? metrics.freshness : null,
        risks: metrics ? metrics.risks : [],
      };
    });

    res.json(finalMetadata);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

module.exports = router;
