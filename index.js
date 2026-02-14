const express = require("express");
const cors = require("cors");

const metadataRoutes = require("./routes/metadata");

const app = express();

app.use(cors());
app.use(express.json());

app.use("/api/metadata", metadataRoutes);

app.listen(5000, () => {
  console.log("Server running on port 5000");
});
