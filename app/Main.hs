{-# LANGUAGE BlockArguments #-}
{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Main (main) where

import Network.Network
import Network.Simple.TCP
import Options.Applicative
import Storage.Storage

defaultRedisPort :: ServiceName
defaultRedisPort = "6379"

readArgs :: ParserInfo AppArgs
readArgs =
  info
    ( AppArgs
        <$> strOption
          ( long "port"
              <> short 'p'
              <> showDefault
              <> value "6379"
              <> help "port"
          )
        <*> strOption
          ( long "replicaof"
              <> help "write adress of master instance like: localhost 6380"
          )
    )
    ( fullDesc
        <> progDesc "world's fastest in-memory database"
        <> header "Redis - an open-source, in-memory data structure store used as a database, cache, and message broker"
    )

main :: IO ()
main = do
  hSetBuffering stdout NoBuffering
  hSetBuffering stderr NoBuffering
  args <- execParser readArgs
  storage <- newStorage (replicaof args)
  runServer args storage