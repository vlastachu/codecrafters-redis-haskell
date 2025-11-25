module Data.AppArgs where

import Network.Simple.TCP

data AppArgs = AppArgs {port :: ServiceName, replicaof :: Text}