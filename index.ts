import fs from 'fs';
import csvParser from 'csv-parser';
import { createObjectCsvWriter } from 'csv-writer';

const rawCsvPath = './raw-data/NOPD_Body_Worn_Camera_Metadata.csv';

interface RawIncident {
  evidence_id: string;
  status: string;
  title: string;
  id_external: string;
  date_uploaded: string;
  created_date_record_start: string;
  date_record_end: string;
  date_deleted: string;
  size_mb: string;
  duration_seconds: string;
  deletion_type: string;
  location: string;
  latitude: string;
  longitude: string;
  police_district: string;
}

const headers = [
  'evidence_id',
  'status',
  'title',
  'id_external',
  'date_uploaded',
  'created_date_record_start',
  'date_record_end',
  'date_deleted',
  'size_mb',
  'duration_seconds',
  'deletion_type',
  'location',
  'latitude',
  'longitude',
  'police_district',
];

async function main() {
  const writers: { [key: string]: any } = {};

  fs.createReadStream(rawCsvPath)
    .pipe(csvParser())
    .on('data', async function (data: RawIncident) {
      const startDate = new Date(data.created_date_record_start);
      const monthKey = `${startDate.getFullYear()}-${startDate.getMonth() + 1}`;

      if (!writers[monthKey]) {
        writers[monthKey] = createObjectCsvWriter({
          path: `./data-by-month/${monthKey}.csv`,
          header: headers.map((header) => ({ id: header, title: header })),
        });
      }

      writers[monthKey].writeRecords([data]);
    })
    .on('end', function () {
      console.log('>>>>>>> complete');
    });
}

main();

// 4 decimal places
