import type { S3Event } from 'aws-lambda';
import { getConfig } from './config.js';
import * as processor from './processor.js';
import { getObjectLineIterator } from './s3.js';

type HandlerResponse = {
  statusCode: number;
  body: string;
};

export const handler = async (
  event: S3Event | Record<string, unknown> = {}
): Promise<HandlerResponse> => {
  const records = Array.isArray((event as S3Event).Records)
    ? (event as S3Event).Records
    : [];
  const recordCount = records.length;

  console.info('Matomo CloudFront Lambda invoked', { records: recordCount });

  try {
    const s3Records = records.filter(
      (r) => r?.s3?.bucket?.name && r?.s3?.object?.key
    );

    if (s3Records.length > 0) {
      const config = getConfig();
      const siteIdMap = config.siteIdMap ?? {};
      const hasSiteIdMap = Object.keys(siteIdMap).length > 0;
      let processed = 0;
      for (const record of s3Records) {
        const bucket = record.s3.bucket.name;
        const key = decodeURIComponent(record.s3.object.key);
        let siteId: number | undefined;
        if (hasSiteIdMap) {
          const index = Object.keys(siteIdMap).find(prefix => key.startsWith(prefix + '/'));
          if (!index) {
            console.warn('No site ID configured for S3 prefix, skipping', {
              key,
              bucket
            });
            continue;
          }
          siteId = siteIdMap[index];
          console.log(`Processing ${index} as SiteID ${siteId}`);
        } else {
          siteId = config.matomoSiteId;
        }
        const lines = await getObjectLineIterator(bucket, key, config.logLevel);
        await processor.sendLogLinesToMatomo(lines, {
          ...config,
          matomoSiteId: siteId
        });
        processed += 1;
      }
      return {
        statusCode: 200,
        body: JSON.stringify({ ok: true, records: recordCount, processed })
      };
    }

    return {
      statusCode: 200,
      body: JSON.stringify({ ok: true, records: recordCount })
    };
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error('Handler error', { error: message });
    return {
      statusCode: 500,
      body: JSON.stringify({ ok: false, error: message })
    };
  }
};
