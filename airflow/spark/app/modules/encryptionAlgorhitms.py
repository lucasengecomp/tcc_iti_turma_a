import base64
import hashlib
from Crypto import Random
from Cryptodome.Cipher import AES
from cryptography.fernet import Fernet

class AESCipher(object):
   
    @staticmethod
    def _pad(s): 
        return s + (AES.block_size - len(s) % AES.block_size) * "}"  #chr(AES.block_size - len(s) % AES.block_size)

    @staticmethod
    def _unpad(s):
        return s[:-ord(s[len(s)-1:])]

class AESNonDeterministicCipher(object):

    @staticmethod
    def encrypt(key, raw):
        key = hashlib.sha256(key.encode()).digest()
        raw = AESCipher._pad(raw)

        salt = Random.new().read(AES.block_size)
        cipher = AES.new(key, AES.MODE_CBC, salt)

        return base64.b64encode(salt + cipher.encrypt(raw.encode()))

    @staticmethod
    def decrypt(key, enc):
        key = hashlib.sha256(key.encode()).digest()
        enc = base64.b64decode(enc)

        salt = enc[:AES.block_size]
        cipher = AES.new(key, AES.MODE_CBC, salt)

        return AESCipher._unpad(cipher.decrypt(enc[AES.block_size:])).decode("utf-8")


class AESDeterministicCipher(object):

    @staticmethod
    def encrypt(key, raw):
        key = hashlib.sha256(key.encode()).digest()
        raw = AESCipher._pad(raw)

        cipher = AES.new(key, AES.MODE_SIV)

        cypherText, _ =  cipher.encrypt_and_digest(raw.encode())

        return cypherText

    @staticmethod
    def decrypt(key, enc):
        key = hashlib.sha256(key.encode()).digest()

        cipher = AES.new(key, AES.MODE_SIV)

        return AESCipher._unpad(cipher.decrypt_and_verify(enc)).decode("utf-8")

class FernetCipher(object):

    @staticmethod
    def getKey():
        return Fernet.generate_key()

    @staticmethod
    def encrypt(key, raw):
        cipher = Fernet(key)
        message = raw.encode('utf-8')

        return cipher.encrypt(message).decode("utf-8")


    @staticmethod
    def decrypt(key, enc):
        cipher = Fernet(key)

        return cipher.decrypt(enc)

class MD5Hash(object):

    @staticmethod
    def encode(raw):
        raw = raw.encode("utf-8")        

        return hashlib.md5(raw).hexdigest()

