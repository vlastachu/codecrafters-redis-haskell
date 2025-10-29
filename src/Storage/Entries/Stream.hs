module Storage.Entries.Stream where

import Control.Concurrent
import qualified Control.Exception as E
import qualified Control.Monad.STM as STM
import Data.Request (StreamEntryKey (..))
import Data.Time.Clock.POSIX (getPOSIXTime)
import qualified StmContainers.Map as SM
import Storage.Definition
import qualified Storage.Entry as SE

-- | Добавить запись в поток.
-- Принимает готовый timestamp (в миллисекундах) и список полей.
-- seqNum всегда 0
addStreamEntry :: SE.StreamID -> [(ByteString, ByteString)] -> [SE.StreamEntry] -> [SE.StreamEntry]
addStreamEntry entryId fields' sdata =
  SE.StreamEntry entryId fields' : sdata

getTimestampMs :: IO Word64
getTimestampMs = do
  t <- getPOSIXTime
  pure $ floor (t * 1000)

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
xadd _ _ (Explicit x y) _ | (x, y) <= (0, 0) = pure $ Left "ERR The ID specified in XADD must be greater than 0-0"
xadd store key entryKey entries = do
  timestamp <- if entryKey == Autogenerate then getTimestampMs else pure 0
  let runStm = do
        stream <- getStream store key
        let maybeLastId = SE.entryID <$> listToMaybe stream
        let mNewId = newStreamId entryKey timestamp maybeLastId
        case mNewId of
          Nothing | entryKey == Autogenerate -> throwSTM RetryRequest -- бросаем исключение чтобы роллить новый timestamp
          Nothing | otherwise -> pure $ Left "ERR The ID specified in XADD is equal or smaller than the target stream top item"
          Just newId | otherwise -> do
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
    -- The only exception is when the time part is 0.
    -- In that case, the default sequence number starts at 1
    nextStreamId 0 Nothing = Just $ SE.StreamID 0 1
    nextStreamId ts Nothing = Just $ SE.StreamID ts 0

    explicitStreamId :: Word64 -> Word64 -> Maybe SE.StreamID -> Maybe SE.StreamID
    explicitStreamId ts seqNum (Just lastId)
      | streamId > lastId = Just streamId
      | otherwise = Nothing
      where
        streamId = SE.StreamID ts seqNum
    explicitStreamId ts seqNum Nothing = Just $ SE.StreamID ts seqNum

xrange :: Storage -> ByteString -> SE.StreamID -> SE.StreamID -> IO [SE.StreamEntry]
xrange storage key from to = defaultAtomically [] $ do
  stream <- getStream storage key
  pure $
    stream
      & dropWhile (\(SE.StreamEntry entryId _) -> entryId > to)
      & takeWhile (\(SE.StreamEntry entryId _) -> entryId >= from)
      & reverse

xread :: Storage -> [(ByteString, SE.StreamID)] -> IO [(ByteString, [SE.StreamEntry])]
xread storage keyIds = defaultAtomically [] $ mapM (readStream storage) keyIds

readStream :: Storage -> (ByteString, SE.StreamID) -> STM (ByteString, [SE.StreamEntry])
readStream storage (key, from) = do
  stream <- getStream storage key
  let streamTail =
        stream
          & takeWhile (\(SE.StreamEntry entryId _) -> entryId >= from)
          & reverse
  pure (key, streamTail)

xreadBlock :: Storage -> ByteString -> Int -> SE.StreamID -> IO (Maybe [SE.StreamEntry])
xreadBlock storage key timeout entryId = do
  let timeoutMap = blockedWaiters storage
  when (timeout > 0) $ void . forkIO $ do
    threadDelay (timeout * 1000)
    safeAtomically $ SM.insert True key timeoutMap
  defaultAtomically Nothing $ do
    (_, entries) <- readStream storage (key, entryId)
    if entries /= []
      then pure (Just entries)
      else
        if timeout > 0
          then do
            mTimeout <- SM.lookup key timeoutMap
            case mTimeout of
              Just True -> pure Nothing
              _ -> STM.retry
          else STM.retry