module Storage.Storage where

import Control.Concurrent
import qualified Data.Map.Strict as M
import Data.Maybe (fromMaybe)
import Data.Sequence (Seq, (><), (|>))
import qualified Data.Sequence as Seq
import Data.Time.Clock
import qualified StmContainers.Map as SM

data Storage = Storage
  { storeMap :: !(SM.Map ByteString (ByteString, Maybe UTCTime)),
    storeArrayMap :: !(SM.Map ByteString (Seq ByteString))
  }

-- | Создать новое хранилище
newStorage :: IO Storage
newStorage = do
  m <- SM.newIO
  m2 <- SM.newIO
  pure $ Storage m m2

-- | Установить ключ с значением и опциональным временем жизни (миллисекунды)
setValue :: Storage -> ByteString -> ByteString -> Maybe Int -> IO ()
setValue store key val msec = do
  now <- getCurrentTime
  let addMilliseconds ms =
        addUTCTime (fromRational (toRational ms / 1000)) now
  let !expireTime = addMilliseconds <$> msec
  let !entry = (val, expireTime) -- строго
  atomically $ SM.insert entry key (storeMap store)

-- | Получить значение по ключу (Nothing, если не найдено или истекло)
getValue :: Storage -> ByteString -> IO (Maybe ByteString)
getValue store key = do
  now <- getCurrentTime
  atomically $ do
    mVal <- SM.lookup key (storeMap store)
    case mVal of
      Nothing -> pure Nothing
      Just (val, Nothing) -> pure (Just val)
      Just (val, Just expTime) ->
        if now < expTime
          then val `seq` pure (Just val)
          else do
            SM.delete key (storeMap store)
            pure Nothing

rpush :: Storage -> ByteString -> [ByteString] -> IO Integer
rpush store key vals = modifySeq store key (\seq -> seq >< fromList vals)

lpush :: Storage -> ByteString -> [ByteString] -> IO Integer
lpush store key vals = modifySeq store key (\seq -> fromList vals >< seq)

modifySeq :: Storage -> ByteString -> (Seq ByteString -> Seq ByteString) -> IO Integer
modifySeq store key modifier = atomically $ do
  let arrayMap = storeArrayMap store
  mSeq <- SM.lookup key arrayMap
  let seq = fromMaybe mempty mSeq
  let modifiedSeq = modifier seq
  SM.insert modifiedSeq key arrayMap
  pure $ toInteger $ length modifiedSeq

getRange :: Storage -> ByteString -> Int -> Int -> IO [ByteString]
getRange store key from to = atomically $ do
  mSeq <- SM.lookup key $ storeArrayMap store
  let seq = fromMaybe mempty mSeq
  let len = length seq
  let from' = if from < 0 then len + from else from
  let to' = if to < 0 then len + to else to
  pure $
    seq
      & Seq.drop from'
      & Seq.take (to' - from' + 1)
      & toList
