module Storage.Entries.Stream where

import Control.Concurrent
import Control.Concurrent.STM (retry)
import qualified Control.Exception as E
import Data.Protocol.Types (RedisValue (..))
import Data.Request (StreamEntryKey (..))
import Data.Time.Clock.POSIX (getPOSIXTime)
import Logic.TxStep (TxStep, txStepFromA)
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

xadd :: Storage -> ByteString -> StreamEntryKey -> [(ByteString, ByteString)] -> TxStep
xadd _ _ (Explicit x y) _ | (x, y) <= (0, 0) = txStepFromA $ ErrorString "ERR The ID specified in XADD must be greater than 0-0"
xadd store key entryKey entries = do
  timestamp <- if entryKey == Autogenerate then getTimestampMs else pure 0
  let runStm = do
        stream <- getStream store key
        let maybeLastId = SE.entryID <$> listToMaybe stream
        let mNewId = newStreamId entryKey timestamp maybeLastId
        case mNewId of
          Nothing -> pure $ ErrorString "ERR The ID specified in XADD is equal or smaller than the target stream top item"
          Just newId -> do
            let newStream = addStreamEntry newId entries stream
            setStream store key newStream
            pure $ BulkString $ show newId
  return (pure (), runStm)

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

xrange :: Storage -> ByteString -> SE.StreamID -> SE.StreamID -> STM RedisValue
xrange storage key from to = do
  stream <- getStream storage key
  pure $
    stream
      & dropWhile (\(SE.StreamEntry entryId _) -> entryId > to)
      & takeWhile (\(SE.StreamEntry entryId _) -> entryId >= from)
      & reverse
      & map formatStreamEntry
      & Array

formatStreamEntry :: SE.StreamEntry -> RedisValue
formatStreamEntry (SE.StreamEntry entryId keyValues) = Array [BulkString (show entryId), Array $ formatKeyValue =<< keyValues]

formatKeyValue :: (ByteString, ByteString) -> [RedisValue]
formatKeyValue (key', value) = [BulkString key', BulkString value]

xread :: Storage -> [(ByteString, SE.StreamID)] -> STM RedisValue
xread storage keyIds = do
  let keyEntriesToArray (key, entries) = Array [BulkString key, Array $ formatStreamEntry <$> entries]
  entries <- mapM (readStream storage) keyIds
  pure $ Array $ keyEntriesToArray <$> entries

readStream :: Storage -> (ByteString, SE.StreamID) -> STM (ByteString, [SE.StreamEntry])
readStream storage (key, from) = do
  stream <- getStream storage key
  let streamTail =
        stream
          & takeWhile (\(SE.StreamEntry entryId _) -> entryId > from)
          & reverse
  pure (key, streamTail)

xreadBlock :: Storage -> ByteString -> Int -> Maybe SE.StreamID -> TxStep
xreadBlock storage key timeout mEntryId = do
  cancelFlag <- newTVarIO False
  chosenEntryId <- case mEntryId of
    Just i -> pure i
    Nothing -> do
      stream <- atomically $ getStream storage key
      let maybeID = SE.entryID <$> listToMaybe stream
      pure $ fromMaybe (SE.StreamID 0 0) maybeID
  let format e = Array [Array [BulkString key, Array $ formatStreamEntry <$> e]]
  putStrLn "HUY HUY"
  print timeout
  let ioAction = when (timeout > 0) $ void . forkIO $ do
        putStrLn "HEY HEY"
        threadDelay (timeout * 1000)
        putStrLn "HE HE"
        safeAtomically $ writeTVar cancelFlag True
  let stmAction = do
        (_, entries) <- readStream storage (key, chosenEntryId)
        if entries /= []
          then pure (format entries)
          else do
            isCanceled <- readTVar cancelFlag
            if isCanceled then pure NilArray else retry
  pure (ioAction, stmAction)
