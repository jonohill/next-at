use super::structure::{
    GtfsAgency, GtfsCalendar, GtfsCalendarDate, GtfsFeedInfo, GtfsRoute, GtfsShape, GtfsStop,
    GtfsStopTime, GtfsTrip,
};
use async_stream::stream;
use async_zip::{base::read::mem::ZipFileReader, error::ZipError};
use csv_async::{AsyncDeserializer, ByteRecord, Trim};
use futures_util::{AsyncRead, Stream, StreamExt};
use itertools::Itertools;

#[derive(thiserror::Error, Debug)]
pub enum GtfsError {
    #[error("CSV error: {0}")]
    Csv(#[from] csv_async::Error),

    #[error(transparent)]
    Request(#[from] reqwest::Error),

    #[error("Zip error: {0}")]
    Zip(#[from] ZipError),
}

pub type GtfsResult<T> = Result<T, GtfsError>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum GtfsItem {
    Agency(GtfsAgency),
    Calendar(GtfsCalendar),
    CalendarDate(GtfsCalendarDate),
    FeedInfo(GtfsFeedInfo),
    Route(GtfsRoute),
    Shape(GtfsShape),
    Stop(GtfsStop),
    StopTime(GtfsStopTime),
    Trip(GtfsTrip),
}

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use GtfsItem::*;

trait DeserialzeItems<'_r, D>
where
    D: DeserializeOwned + '_r,
{
    async fn items<F>(
        self,
        from_record: u64,
        f: F,
    ) -> GtfsResult<impl Stream<Item = GtfsResult<(GtfsItem, u64)>>>
    where
        F: FnMut(D) -> GtfsItem;
}

impl<'_r, R, D> DeserialzeItems<'_r, D> for AsyncDeserializer<R>
where
    R: AsyncRead + Unpin + Send + '_r,
    D: DeserializeOwned + '_r,
{
    async fn items<F>(
        self,
        from_record: u64,
        mut f: F,
    ) -> GtfsResult<impl Stream<Item = GtfsResult<(GtfsItem, u64)>>>
    where
        F: FnMut(D) -> GtfsItem,
    {
        let mut this = self;

        // skip through the records if requested
        if from_record > 0 {
            log::info!("Skipping {} records", from_record);
            let mut record = ByteRecord::new();
            for _ in 0..from_record {
                this.read_byte_record(&mut record).await?;
            }
        }

        let mapped = this.into_deserialize_with_pos().map(move |(item, pos)| {
            item.map(|item| (f(item), pos.record()))
                .map_err(GtfsError::Csv)
        });
        Ok(mapped)
    }
}

/// Given a zip file, return a stream which yields GTFS items
/// * `include` - An ordered list of files to include in the string to the record in the file to start reading from. Set it to 0 if not resuming.
pub fn read_gtfs_from_zip(
    zip: ZipFileReader,
    include: Vec<(String, u64)>,
) -> impl Stream<Item = GtfsResult<(GtfsItem, u64)>> {
    stream! {

        // Initial pass to get ids so that files can be returned in requested order
        let mut file_list = Vec::new();

        for idx in 0..usize::MAX {

            let reader = match zip.reader_with_entry(idx).await {
                Ok(entry) => Ok(entry),
                Err(ZipError::EntryIndexOutOfBounds) => break,
                Err(e) => Err(e),
            }?;

            let filename = reader.entry().filename().clone().into_string()?;
            
            let (req_pos, offset) = match include.iter().enumerate().find(|(_, (name, _))| name == &filename) {
                Some((req_pos, (_, offset))) => (req_pos, *offset),
                None => {
                    log::debug!("Skipping {}", filename);
                    continue;
                }
            };

            file_list.push((req_pos, idx, filename, offset));
        }

        let file_list = file_list.into_iter()
            .sorted()
            .map(|(_, idx, filename, offset)| (idx, filename, offset))
            .collect::<Vec<_>>();

        for (idx, filename, offset) in file_list {
            
            log::debug!("Reading {}", filename);

            let reader = zip.reader_with_entry(idx).await?;

            let csv = csv_async::AsyncReaderBuilder::new()
                .flexible(true)
                .trim(Trim::All)
                .create_deserializer(reader);

            // I haven't been able to figure out a way to do this without repeating the yielding inside the match
            // because otherwise the return from the match is of different types in each arm
            match filename.as_str() {
                "agency.txt" => {
                    let mut items = csv.items(offset, Agency).await?;
                    while let Some(item) = items.next().await {
                        yield item;
                    }
                },
                "calendar_dates.txt" => {
                    let mut items = csv.items(offset, CalendarDate).await?;
                    while let Some(item) = items.next().await {
                        yield item;
                    }
                },
                "calendar.txt" => {
                    let mut items = csv.items(offset, Calendar).await?;
                    while let Some(item) = items.next().await {
                        yield item;
                    }
                },
                "feed_info.txt" => {
                    let mut items = csv.items(offset, FeedInfo).await?;
                    while let Some(item) = items.next().await {
                        yield item;
                    }
                },
                "routes.txt" => {
                    let mut items = csv.items(offset, Route).await?;
                    while let Some(item) = items.next().await {
                        yield item;
                    }
                },
                "shapes.txt" => {
                    let mut items = csv.items(offset, Shape).await?;
                    while let Some(item) = items.next().await {
                        yield item;
                    }
                },
                "stops.txt" => {
                    let mut items = csv.items(offset, Stop).await?;
                    while let Some(item) = items.next().await {
                        yield item;
                    }
                },
                "stop_times.txt" => {
                    let mut items = csv.items(offset, StopTime).await?;
                    while let Some(item) = items.next().await {
                        yield item;
                    }
                },
                "trips.txt" => {
                    let mut items = csv.items(offset, Trip).await?;
                    while let Some(item) = items.next().await {
                        yield item;
                    }
                },
                _ => {},
            };
        }
    }
}

pub async fn get_gtfs_zip_from_url(
    url: &str,
    if_modified_since: Option<String>,
) -> GtfsResult<
    Option<(
        Option<String>,
        ZipFileReader,
    )>,
> {
    let resp = reqwest::get(url).await?
        .error_for_status()?;

    let last_modified = resp.headers().get("last-modified")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.to_string());

    // Check the last modified header here - Azure block storage seems to ignore If-Modified-Since/If-None-Match!
    if if_modified_since == last_modified {
        return Ok(None);
    }

    // I'd prefer to stream the response into the zip reader
    // however it appears, at least for the tested zips, that they are are compressed
    // in a way that requires the dictionary (end of the file) to be read
    // If memory usage becomes an issue, consider reading ranges from the server

    let bytes = resp.bytes().await?;

    let zip_reader = ZipFileReader::new(bytes.into()).await?;

    Ok(Some((last_modified, zip_reader)))
}
