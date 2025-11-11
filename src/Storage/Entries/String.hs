module Storage.Entries.String where

import Control.Concurrent
import qualified Data.ByteString.Char8 as BS
import qualified StmContainers.Map as SM
import Storage.Definition
import qualified Storage.Entry as SE
import Data.Protocol.Types
import Logic.TxStep (TxStep)

setValue :: Storage -> ByteString -> ByteString -> Maybe Int -> TxStep
setValue store key val maybemsec = pure (finalizer, stmAction)
  where
     !entry = SE.String val
     finalizer = setDestroyTimer store key maybemsec
     stmAction = do
      SM.insert entry key (storeMap store)
      pure $ SimpleString "OK"
  

setDestroyTimer :: Storage -> ByteString -> Maybe Int -> IO ()
setDestroyTimer _ _ Nothing = pure ()
setDestroyTimer store key (Just msec) = do
  _ <- forkIO $ do
    threadDelay (msec * 1000) -- переводим миллисекунды в микросекунды
    safeAtomically $ do
      SM.delete key (storeMap store)
  pure ()

getValueSTM :: Storage -> ByteString -> STM (Maybe ByteString)
getValueSTM store key = do
  mVal <- SM.lookup key (storeMap store)
  case mVal of
    Just (SE.String val) -> pure $ Just val
    Nothing -> pure Nothing
    other -> throwSTM (TypeMismatch $ "Expected string, but got " <> show other)

getValue :: Storage -> ByteString -> STM RedisValue
getValue store key = maybe NilString BulkString <$> getValueSTM store key

incValue :: Storage -> ByteString -> STM RedisValue
incValue store key = do
    mVal <- getValueSTM store key
    let val = fromMaybe "0" mVal
    let toInt i = readMaybe $ BS.unpack i
    case (+ 1) <$> toInt val of
      Just i -> do
        SM.insert (SE.String $ show i) key (storeMap store)
        pure $ Integer i
      Nothing -> pure $ ErrorString "ERR value is not an integer or out of range"
