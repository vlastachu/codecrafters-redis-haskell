{-# LANGUAGE BlockArguments #-}
{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Main (main) where

import Network.Network
import Storage.Storage

defaultRedisPort :: ServiceName
defaultRedisPort = "6379"

getArgByName :: String -> [String] -> Maybe String
getArgByName _ [] = Nothing
getArgByName _ [_] = Nothing
getArgByName name (name: arg: rest) = Just arg
getArgByName name (_: rest) = getArgByName name rest


readPort :: IO ServiceName
readPort = do
  args <- getArgs
  let port = fromMaybe defaultRedisPort $ getArgByName "--port"
  pure port

main :: IO ()
main = do
  hSetBuffering stdout NoBuffering
  hSetBuffering stderr NoBuffering
  port <- readPort
  storage <- newStorage
  runServer port defaultRedisPort storage