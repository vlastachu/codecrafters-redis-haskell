{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import System.IO (hPutStrLn, hSetBuffering, stdout, stderr, BufferMode(NoBuffering))

import Network.Network
import Storage.Storage

main :: IO ()
main = do
    hSetBuffering stdout NoBuffering
    hSetBuffering stderr NoBuffering
    storage <- newStorage
    runServer storage