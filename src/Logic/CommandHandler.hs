module Logic.CommandHandler where

import Data.Request
import Data.Response
import Storage.Storage

handleCommand :: Storage -> Request -> IO Response
handleCommand _ Ping = pure Pong
handleCommand _ (Echo str) = pure $ RawString str
handleCommand store (Get key) = do
  mVal <- getValue store key
  pure $ maybe Nil RawString mVal
handleCommand store (Type key) = do
  mVal <- getValue store key
  pure $ RawString $ case mVal of
    Just _ -> "string"
    Nothing -> "none"
handleCommand store (Set key val mExp) = do
  setValue store key val mExp
  pure OK
handleCommand store (LPush key vals) = do
  len <- lpush store key vals
  pure $ RawInteger len
handleCommand store (RPush key vals) = do
  len <- rpush store key vals
  pure $ RawInteger len
handleCommand store (LRange key from to) = do
  range <- getRange store key from to
  pure $ RawArray $ RawString <$> range
handleCommand store (LLen key) = do
  len <- llen store key
  pure $ RawInteger len
handleCommand store (LPop key len) = do
  range <- lpop store key len
  pure $ case range of
    [] -> Nil
    [oneString] -> RawString oneString
    more -> RawArray $ RawString <$> more
handleCommand store (BLPop key timeout) = do
  item <- blpop store key timeout
  pure $ maybe RawNilArray (RawArray . (RawString <$>)) item
handleCommand store (XADD key entries) = do
  timestamp <- xadd store key entries
  pure $ RawString timestamp
