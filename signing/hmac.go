package signing

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"log"
	"net/url"
)

// SignURL Signs a given url with a given key. The provided url is encoded in full length based on the generated string.
// Before signing a random salt is added to the query parameters of the url. The Signature is added to the query parameters after calculating
// the signature. The signature is calculated using Hmac with SHA256.
func SignURL(key []byte, baseURL *url.URL) (*url.URL, error) {
	q := baseURL.Query()
	saltBytes := make([]byte, 64)
	_, err := rand.Read(saltBytes)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	escapedSalt := url.QueryEscape(string(saltBytes))
	q.Set("salt", escapedSalt)

	baseURL.RawQuery = q.Encode()
	signature, err := HMAC_sha256(key, []byte(baseURL.String()))
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	q.Set("sign", string(signature))

	baseURL.RawQuery = q.Encode()

	return baseURL, nil
}

func HMAC_sha256(key, msg []byte) ([]byte, error) {
	mac := hmac.New(sha256.New, key)

	_, err := mac.Write(msg)
	if err != nil {
		return nil, err
	}

	return mac.Sum(nil), nil
}

func VerifyHMAC_sha256(key string, url *url.URL) (bool, error) {
	queryParams := url.Query()
	signature := queryParams.Get("sign")
	queryParams.Del("sign")
	url.RawQuery = queryParams.Encode()

	calculatedSignature, err := HMAC_sha256([]byte(key), []byte(url.String()))
	if err != nil {
		return false, err
	}

	if !hmac.Equal([]byte(signature), calculatedSignature) {
		return false, nil
	}

	return true, nil
}
