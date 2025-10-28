module Storage.Entries.Stream where

import qualified Control.Exception as E
import Data.Request (StreamEntryKey (..))
import GHC.Clock (getMonotonicTimeNSec)
import qualified StmContainers.Map as SM
import Storage.Definition
import qualified Storage.Entry as SE

-- | Добавить запись в поток.
-- Принимает готовый timestamp (в миллисекундах) и список полей.
-- seqNum всегда 0
addStreamEntry :: SE.StreamID -> [(ByteString, ByteString)] -> [SE.StreamEntry] -> [SE.StreamEntry]
addStreamEntry entryId fields' sdata =
  SE.StreamEntry entryId fields' : sdata

getTimestampNs :: IO Word64
getTimestampNs = getMonotonicTimeNSec

lastID :: [SE.StreamEntry] -> STM ByteString
lastID (lastItem : _) = pure $ show $ SE.entryID lastItem
lastID _ = throwSTM $ NotFound "empty stream"

setStream :: Storage -> ByteString -> [SE.StreamEntry] -> STM ()
setStream store key val = SM.insert (SE.Stream val) key (storeMap store)

-- convertRequestKeyToStorageKey :: StreamEntryKey -> IO SE.StreamID
-- convertRequestKeyToStorageKey streamEntryKey = case streamEntryKey of
--     Autogenerate

getStream :: Storage -> ByteString -> STM [SE.StreamEntry]
getStream store key = do
  mStream <- SM.lookup key (storeMap store)
  case mStream of
    Just (SE.Stream stream) -> pure stream
    Nothing -> pure mempty
    value -> throwSTM (TypeMismatch $ "Expected stream, but got: " <> show value)

xadd :: Storage -> ByteString -> StreamEntryKey -> [(ByteString, ByteString)] -> IO (Either ByteString ByteString)
xadd store key entryKey entries = do
  timestamp <- if entryKey == Autogenerate then getTimestampNs else pure 0
  let runStm = do
        stream <- getStream store key
        let maybeLastId = SE.entryID <$> listToMaybe stream
        case newStreamId entryKey timestamp maybeLastId of
          Nothing | entryKey == Autogenerate -> throwSTM RetryRequest -- бросаем исключение чтобы роллить новый timestamp
          Nothing | otherwise -> pure $ Left "ERR The ID specified in XADD is equal or smaller than the target stream top item"
          Just newId -> do
            let newStream = addStreamEntry newId entries stream
            setStream store key newStream
            pure $ Right $ show newId
  atomically runStm `E.catch` \case
    RetryRequest -> xadd store key entryKey entries -- повторяем для RetryRequest
    e -> do
      putStrLn $ "[StorageError] " <> show (e :: StorageError)
      pure $ Left $ show e

newStreamId :: StreamEntryKey -> Word64 -> Maybe SE.StreamID -> Maybe SE.StreamID
newStreamId key timestamp mLast = case key of
  Autogenerate -> nextStreamId timestamp mLast
  AutogenerateSequenceNumber explicitTimestamp -> nextStreamId explicitTimestamp mLast
  Explicit ts seqNum -> explicitStreamId ts seqNum mLast
  where
    nextStreamId :: Word64 -> Maybe SE.StreamID -> Maybe SE.StreamID
    nextStreamId ts (Just (SE.StreamID lastTs lastSeq))
      | ts > lastTs = Just $ SE.StreamID ts 0
      | ts == lastTs = Just $ SE.StreamID ts (lastSeq + 1)
      | otherwise = Nothing
    nextStreamId ts Nothing = Just $ SE.StreamID ts 0

    explicitStreamId :: Word64 -> Word64 -> Maybe SE.StreamID -> Maybe SE.StreamID
    explicitStreamId ts seqNum (Just lastId)
      | streamId > lastId = Just streamId
      | otherwise = Nothing
      where
        streamId = SE.StreamID ts seqNum
    explicitStreamId ts seqNum Nothing = Just $ SE.StreamID ts seqNum