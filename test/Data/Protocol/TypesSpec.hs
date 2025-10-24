module Data.Protocol.TypesSpec where

import qualified Data.ByteString as BS
import Data.Protocol.Decode
import Data.Protocol.Encode
import Data.Protocol.Types
import Test.QuickCheck

-- | Генератор произвольных RedisValue
instance Arbitrary RedisValue where
  arbitrary = sized genValue

genValue :: Int -> Gen RedisValue
genValue n
  | n <= 0 = oneof base
  | otherwise = oneof (base ++ [genArray])
  where
    base =
      [ SimpleString <$> smallBS,
        ErrorString <$> smallBS,
        BulkString <$> oneof [pure Nothing, Just <$> smallBS],
        Integer <$> arbitrary
      ]
    genArray =
      Array
        <$> oneof
          [ pure Nothing,
            Just <$> resize (n `div` 2) (listOf (genValue (n `div` 2)))
          ]
    smallBS = BS.pack <$> listOf (choose (33, 126)) -- печатные ASCII

prop_encodeDecodeRedisValue :: RedisValue -> Bool
prop_encodeDecodeRedisValue v =
  case decode (encode v) of
    Right v' -> v' == v
    Left _ -> False
