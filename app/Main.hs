{-# LANGUAGE BlockArguments #-}
{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Main (main) where

import Network.Network
import Network.Simple.TCP
import Options.Applicative
import Storage.Storage

defaultRedisPort :: ServiceName
defaultRedisPort = "6379"

newtype AppArgs = AppArgs {port :: ServiceName}

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
  let port' = port args
  storage <- newStorage
  runServer port' storage