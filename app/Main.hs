-- {-# LANGUAGE ScopedTypeVariables #-}
-- import Control.Concurrent (forkIO, threadDelay)
-- import Control.Concurrent.STM (retry)
-- import Text.Printf
-- main :: IO ()
-- main = do
--   tvarArr <- newTVarIO [] -- наш общий массив
--   doneVar <- newEmptyMVar -- сигнал завершения
--   let numThreads = 1000 :: Int
--   -- запускаем треды
--   forM_ [1 .. numThreads] $ \i -> do
--     threadDelay 5000 -- 5 мс между спавнами потоков
--     forkIO $ do
--       val <- atomically $ do
--         arr <- readTVar tvarArr
--         case arr of
--           [] -> retry
--           (x : xs) -> do
--             writeTVar tvarArr xs
--             return x
--       printf "Thread %04d got %d\n" i val
--       when (i == numThreads) $ putMVar doneVar ()
--   -- теперь добавляем элементы с задержкой 5 мс
--   forM_ [1 .. numThreads] $ \x -> do
--     atomically $ do
--       arr <- readTVar tvarArr
--       writeTVar tvarArr (arr ++ [x])
--     threadDelay 5000 -- 5 мс между добавлениями
--   -- ждём, пока последний поток сообщит о завершении
--   takeMVar doneVar
{-# LANGUAGE BlockArguments #-}
{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Main (main) where

import Network.Network
import Storage.Storage

main :: IO ()
main = do
  hSetBuffering stdout NoBuffering
  hSetBuffering stderr NoBuffering
  storage <- newStorage
  runServer storage