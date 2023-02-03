use x509_certificate::X509Certificate;

use crate::common::CvmfsError;

pub const CERTIFICATE_ROOT_PREFIX: &str = "X";

struct Certificate {
    openssl_certificate: X509Certificate,
}

impl Certificate {
    fn verify(&self, signature: &str, message: &str) -> bool {
        unimplemented!()
    }
}

impl<'a> TryFrom<&'a [u8]> for Certificate {
    type Error = CvmfsError;

    fn try_from(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        unimplemented!()
    }
}