use x509_certificate::X509Certificate;

use crate::common::CvmfsError;

pub const CERTIFICATE_ROOT_PREFIX: &str = "X";

struct Certificate {
    pub openssl_certificate: X509Certificate,
}

impl Certificate {
    pub fn verify(&self, _signature: &str, _message: &str) -> bool {
        unimplemented!()
    }
}

impl<'a> TryFrom<&'a [u8]> for Certificate {
    type Error = CvmfsError;

    fn try_from(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        Ok(Self {
            openssl_certificate: X509Certificate::from_der(bytes)
                .map_err(|_| CvmfsError::Certificate)?,
        })
    }
}
