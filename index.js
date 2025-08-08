const express = require('express');
const dotenv = require('dotenv');
const db = require('./db');

dotenv.config();
const app = express();
app.use(express.json());

const cors = require('cors');
app.use(cors());


app.post('/log', async (req, res) => {
  const { screen, action, timestamp, kioskId } = req.body;

  if (!screen || !action || !timestamp || !kioskId) {
    return res.status(400).json({ error: 'Missing required fields' });
  }

  try {
    await db.query(
      'INSERT INTO interactions (screen, action, timestamp, kiosk_id) VALUES ($1, $2, $3, $4)',
      [screen, action, timestamp, kioskId]
    );
    res.status(200).json({ message: 'Interaction logged' });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Database error' });
  }
});

app.get('/', (req, res) => {
  res.send('Kiosk API is running');
});

app.get('/all', async (req, res) => {
  try {
    const result = await db.query(' SELECT i.*, k.friendly_name
      FROM interactions i
      LEFT JOIN kiosk_locations k ON i.kiosk_id = k.kiosk_id
      ORDER BY i.timestamp DESC
      LIMIT 100');
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Failed to fetch logs' });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));