module Logic.CommandHandler where

import Data.Request
import Data.Response
import Storage.Storage

handleCommand :: Storage -> Request -> IO Response
handleCommand _ Ping = pure Pong
handleCommand _ (Echo str) = pure $ RawString $ Just str
handleCommand store (Get key) = do
  mVal <- getValue store key
  pure $ RawString mVal
handleCommand store (Set key val mExp) = do
  setValue store key val mExp
  pure OK
