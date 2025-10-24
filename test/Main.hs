import Data.Protocol.TypesSpec (prop_encodeDecodeRedisValue)
import Test.QuickCheck

main :: IO ()
main = quickCheck prop_encodeDecodeRedisValue
