const express = require('express');
const { Pool } = require('pg');
const cors = require('cors');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
});

// ============ HELPER FUNCTION: Convert JSON to CSV ============
function jsonToCSV(data) {
  if (!data || data.length === 0) {
    return '';
  }
  
  // Get headers from first object
  const headers = Object.keys(data[0]);
  
  // Create CSV header row
  const headerRow = headers.join(',');
  
  // Create data rows
  const dataRows = data.map(row => {
    return headers.map(header => {
      let value = row[header];
      
      // Handle null/undefined
      if (value === null || value === undefined) {
        return '';
      }
      
      // Convert to string
      value = String(value);
      
      // Escape quotes and wrap in quotes if contains comma, quote, or newline
      if (value.includes(',') || value.includes('"') || value.includes('\n')) {
        value = '"' + value.replace(/"/g, '""') + '"';
      }
      
      return value;
    }).join(',');
  });
  
  // Combine header and data
  return [headerRow, ...dataRows].join('\n');
}

// ============ INITIALIZE DATABASE ============
async function initDB() {
  const queries = [
    // Raw events table
    `CREATE TABLE IF NOT EXISTS raw_events (
      id BIGSERIAL PRIMARY KEY,
      timestamp TIMESTAMP DEFAULT NOW(),
      opening_number INT,
      event_type VARCHAR(50),
      event_detail VARCHAR(100),
      raw_value TEXT,
      created_at TIMESTAMP DEFAULT NOW()
    )`,
    
    // Structured bin events table
    `CREATE TABLE IF NOT EXISTS bin_events (
      id BIGSERIAL PRIMARY KEY,
      created_at TIMESTAMP DEFAULT NOW(),
      opening_number INT NOT NULL,
      timestamp TIMESTAMP,
      open_start_time TIMESTAMP,
      open_complete_time TIMESTAMP,
      close_start_time TIMESTAMP,
      close_complete_time TIMESTAMP,
      open_duration_s FLOAT,
      close_duration_s FLOAT,
      total_cycle_s FLOAT,
      start_angle_deg INT,
      max_angle_deg INT,
      end_angle_deg INT,
      avg_speed_deg_s FLOAT,
      lora_packets_received INT,
      lora_packets_missed INT,
      avg_distance_cm FLOAT,
      avg_rssi_dbm FLOAT,
      packet_details TEXT,
      capacitor_voltage_v FLOAT
    )`,
    
    // Indexes
    `CREATE INDEX IF NOT EXISTS idx_raw_opening ON raw_events(opening_number)`,
    `CREATE INDEX IF NOT EXISTS idx_raw_type ON raw_events(event_type)`,
    `CREATE INDEX IF NOT EXISTS idx_bin_opening ON bin_events(opening_number)`,
    `CREATE INDEX IF NOT EXISTS idx_bin_timestamp ON bin_events(timestamp)`
  ];
  
  try {
    for (const query of queries) {
      await pool.query(query);
    }
    console.log('✅ Database initialized');
  } catch (err) {
    console.error('❌ Database init error:', err);
  }
}

// ============ HEALTH CHECK ============
app.get('/', (req, res) => {
  res.json({ 
    status: 'ok', 
    message: 'Smart Bin API Running',
    endpoints: {
      raw_event: 'POST /api/raw-event',
      bin_event: 'POST /api/bin-event',
      raw_csv: 'GET /api/raw-events/csv',
      structured_csv: 'GET /api/bin-events/csv',
      raw_json: 'GET /api/raw-events',
      structured_json: 'GET /api/bin-events'
    }
  });
});

// ============ POST RAW EVENT ============
app.post('/api/raw-event', async (req, res) => {
  const { opening_number, event_type, event_detail, raw_value } = req.body;
  
  try {
    const query = `
      INSERT INTO raw_events (opening_number, event_type, event_detail, raw_value)
      VALUES ($1, $2, $3, $4)
      RETURNING id
    `;
    
    const result = await pool.query(query, [
      opening_number,
      event_type,
      event_detail,
      raw_value || '{}'
    ]);
    
    res.status(201).json({ success: true, id: result.rows[0].id });
  } catch (err) {
    console.error('❌ Raw event error:', err);
    res.status(500).json({ error: 'Database error' });
  }
});

// ============ POST STRUCTURED BIN EVENT ============
app.post('/api/bin-event', async (req, res) => {
  const {
    opening_number,
    timestamp,
    open_start_time,
    open_complete_time,
    close_start_time,
    close_complete_time,
    open_duration_s,
    close_duration_s,
    total_cycle_s,
    start_angle_deg,
    max_angle_deg,
    end_angle_deg,
    avg_speed_deg_s,
    lora_packets_received,
    lora_packets_missed,
    avg_distance_cm,
    avg_rssi_dbm,
    packet_details,
    capacitor_voltage_v
  } = req.body;

  if (!opening_number) {
    return res.status(400).json({ error: 'opening_number is required' });
  }

  try {
    const query = `
      INSERT INTO bin_events (
        opening_number, timestamp,
        open_start_time, open_complete_time, close_start_time, close_complete_time,
        open_duration_s, close_duration_s, total_cycle_s,
        start_angle_deg, max_angle_deg, end_angle_deg, avg_speed_deg_s,
        lora_packets_received, lora_packets_missed,
        avg_distance_cm, avg_rssi_dbm, packet_details, capacitor_voltage_v
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
      RETURNING id
    `;
    
    const values = [
      opening_number,
      timestamp || new Date().toISOString(),
      open_start_time,
      open_complete_time,
      close_start_time,
      close_complete_time,
      open_duration_s,
      close_duration_s,
      total_cycle_s,
      start_angle_deg,
      max_angle_deg,
      end_angle_deg,
      avg_speed_deg_s,
      lora_packets_received,
      lora_packets_missed,
      avg_distance_cm,
      avg_rssi_dbm,
      packet_details,
      capacitor_voltage_v
    ];

    const result = await pool.query(query, values);
    
    console.log(`✅ Saved opening #${opening_number}, ID: ${result.rows[0].id}`);
    res.status(201).json({ 
      success: true, 
      id: result.rows[0].id,
      opening_number 
    });

  } catch (err) {
    console.error('❌ Database error:', err);
    res.status(500).json({ error: 'Database error', details: err.message });
  }
});

// ============ GET RAW EVENTS (JSON) ============
app.get('/api/raw-events', async (req, res) => {
  const limit = parseInt(req.query.limit) || 1000;
  const opening = req.query.opening_number;
  
  try {
    let query = 'SELECT * FROM raw_events';
    let values = [];
    
    if (opening) {
      query += ' WHERE opening_number = $1';
      values.push(opening);
    }
    
    query += ' ORDER BY timestamp DESC LIMIT $' + (values.length + 1);
    values.push(limit);
    
    const result = await pool.query(query, values);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: 'Database error' });
  }
});

// ============ GET STRUCTURED EVENTS (JSON) ============
app.get('/api/bin-events', async (req, res) => {
  const limit = parseInt(req.query.limit) || 100;
  
  try {
    const result = await pool.query(
      'SELECT * FROM bin_events ORDER BY opening_number DESC LIMIT $1',
      [limit]
    );
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: 'Database error' });
  }
});

// ============ DOWNLOAD RAW CSV (NO DEPENDENCIES!) ============
app.get('/api/raw-events/csv', async (req, res) => {
  const opening = req.query.opening_number;
  
  try {
    let query = 'SELECT * FROM raw_events';
    let values = [];
    
    if (opening) {
      query += ' WHERE opening_number = $1';
      values.push(opening);
    }
    
    query += ' ORDER BY timestamp ASC';
    
    const result = await pool.query(query, values);
    
    if (result.rows.length === 0) {
      return res.status(404).send('No data found');
    }
    
    // Convert to CSV using our custom function
    const csv = jsonToCSV(result.rows);
    
    res.header('Content-Type', 'text/csv');
    res.attachment('raw_events.csv');
    res.send(csv);
    
    console.log(`📥 Downloaded RAW CSV: ${result.rows.length} rows`);
  } catch (err) {
    console.error('❌ CSV generation error:', err);
    res.status(500).json({ error: 'CSV generation error' });
  }
});

// ============ DOWNLOAD STRUCTURED CSV (NO DEPENDENCIES!) ============
app.get('/api/bin-events/csv', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM bin_events ORDER BY opening_number ASC');
    
    if (result.rows.length === 0) {
      return res.status(404).send('No data found');
    }
    
    // Convert to CSV using our custom function
    const csv = jsonToCSV(result.rows);
    
    res.header('Content-Type', 'text/csv');
    res.attachment('structured_events.csv');
    res.send(csv);
    
    console.log(`📥 Downloaded STRUCTURED CSV: ${result.rows.length} rows`);
  } catch (err) {
    console.error('❌ CSV generation error:', err);
    res.status(500).json({ error: 'CSV generation error' });
  }
});

// ============ START SERVER ============
app.listen(PORT, async () => {
  await initDB();
  console.log(`🚀 Server running on port ${PORT}`);
  console.log(`📊 RAW CSV: GET /api/raw-events/csv`);
  console.log(`📊 STRUCTURED CSV: GET /api/bin-events/csv`);
});