//! OAuth provider configuration, nested under `Config::oauth` (ARC-012).
//! Each provider parses its own env vars in its own `from_env` (ARC-205); the
//! providers all share the same client_id/client_secret shape plus whatever
//! extra endpoint config that provider needs. Grouping them here (rather than
//! as ~20 flat `Config` fields) is what lets `server/src/auth/{github,google,
//! gitlab,microsoft,apple,oidc}.rs` and their tests build a `Config` literal
//! with `..Default::default()` instead of listing every provider's fields.

/// GitHub OAuth app credentials + API endpoints.
#[derive(Clone, Debug)]
pub struct GithubOAuth {
    pub client_id: Option<String>,     // RTDB_GITHUB_CLIENT_ID
    pub client_secret: Option<String>, // RTDB_GITHUB_CLIENT_SECRET
    pub base_url: String,              // RTDB_GITHUB_BASE_URL, default https://github.com
    pub api_url: String,               // RTDB_GITHUB_API_URL, default https://api.github.com
}

impl Default for GithubOAuth {
    fn default() -> Self {
        Self {
            client_id: None,
            client_secret: None,
            base_url: "https://github.com".to_string(),
            api_url: "https://api.github.com".to_string(),
        }
    }
}

impl GithubOAuth {
    pub(super) fn from_env() -> Self {
        let client_id = std::env::var("RTDB_GITHUB_CLIENT_ID").ok();
        let client_secret = std::env::var("RTDB_GITHUB_CLIENT_SECRET").ok();

        let base_url = std::env::var("RTDB_GITHUB_BASE_URL")
            .unwrap_or_else(|_| "https://github.com".to_string());

        let api_url = std::env::var("RTDB_GITHUB_API_URL")
            .unwrap_or_else(|_| "https://api.github.com".to_string());

        Self {
            client_id,
            client_secret,
            base_url,
            api_url,
        }
    }
}

/// Google OAuth app credentials.
#[derive(Clone, Debug, Default)]
pub struct GoogleOAuth {
    pub client_id: Option<String>,     // RTDB_GOOGLE_CLIENT_ID
    pub client_secret: Option<String>, // RTDB_GOOGLE_CLIENT_SECRET
}

impl GoogleOAuth {
    pub(super) fn from_env() -> Self {
        Self {
            client_id: std::env::var("RTDB_GOOGLE_CLIENT_ID").ok(),
            client_secret: std::env::var("RTDB_GOOGLE_CLIENT_SECRET").ok(),
        }
    }
}

/// GitLab OAuth app credentials + instance base URL.
#[derive(Clone, Debug)]
pub struct GitlabOAuth {
    pub client_id: Option<String>,     // RTDB_GITLAB_CLIENT_ID
    pub client_secret: Option<String>, // RTDB_GITLAB_CLIENT_SECRET
    pub base_url: String,              // RTDB_GITLAB_BASE_URL, default https://gitlab.com
}

impl Default for GitlabOAuth {
    fn default() -> Self {
        Self {
            client_id: None,
            client_secret: None,
            base_url: "https://gitlab.com".to_string(),
        }
    }
}

impl GitlabOAuth {
    pub(super) fn from_env() -> Self {
        let client_id = std::env::var("RTDB_GITLAB_CLIENT_ID").ok();
        let client_secret = std::env::var("RTDB_GITLAB_CLIENT_SECRET").ok();
        let base_url = std::env::var("RTDB_GITLAB_BASE_URL")
            .unwrap_or_else(|_| "https://gitlab.com".to_string());
        Self {
            client_id,
            client_secret,
            base_url,
        }
    }
}

/// Generic OpenID Connect provider (one impl for any standards-compliant
/// IdP: Azure AD, Keycloak, Auth0, Okta, self-hosted). The authorize/token/
/// userinfo URLs come from the IdP's `/.well-known/openid-configuration` — the
/// trait's sync authorize_url can't do live discovery, so endpoints are
/// configuration. Active only when all five are set; else routes return 503.
#[derive(Clone, Debug, Default)]
pub struct OidcProvider {
    pub client_id: Option<String>,     // RTDB_OIDC_CLIENT_ID
    pub client_secret: Option<String>, // RTDB_OIDC_CLIENT_SECRET
    pub authorize_url: Option<String>, // RTDB_OIDC_AUTHORIZE_URL
    pub token_url: Option<String>,     // RTDB_OIDC_TOKEN_URL
    pub userinfo_url: Option<String>,  // RTDB_OIDC_USERINFO_URL
}

impl OidcProvider {
    pub(super) fn from_env() -> Self {
        Self {
            client_id: std::env::var("RTDB_OIDC_CLIENT_ID").ok(),
            client_secret: std::env::var("RTDB_OIDC_CLIENT_SECRET").ok(),
            authorize_url: std::env::var("RTDB_OIDC_AUTHORIZE_URL").ok(),
            token_url: std::env::var("RTDB_OIDC_TOKEN_URL").ok(),
            userinfo_url: std::env::var("RTDB_OIDC_USERINFO_URL").ok(),
        }
    }
}

/// Microsoft (Entra ID / Azure AD v2.0) OAuth provider. Models on the generic
/// OIDC provider but derives Microsoft's well-known authorize/token/userinfo
/// endpoints from `tenant`, so the operator supplies credentials + tenant
/// only (no four-URL paste). RTDB_MICROSOFT_CLIENT_ID /
/// RTDB_MICROSOFT_CLIENT_SECRET / RTDB_MICROSOFT_TENANT (default "common" =
/// any Microsoft account; a tenant GUID/name restricts to one org).
#[derive(Clone, Debug)]
pub struct MicrosoftOAuth {
    pub client_id: Option<String>,     // RTDB_MICROSOFT_CLIENT_ID
    pub client_secret: Option<String>, // RTDB_MICROSOFT_CLIENT_SECRET
    pub tenant: String,                // RTDB_MICROSOFT_TENANT, default "common"
}

impl Default for MicrosoftOAuth {
    fn default() -> Self {
        Self {
            client_id: None,
            client_secret: None,
            tenant: "common".to_string(),
        }
    }
}

impl MicrosoftOAuth {
    pub(super) fn from_env() -> Self {
        let client_id = std::env::var("RTDB_MICROSOFT_CLIENT_ID").ok();
        let client_secret = std::env::var("RTDB_MICROSOFT_CLIENT_SECRET").ok();
        // `tenant` defaults to "common" (any Microsoft account); an empty
        // value falls back to that default so a blank RTDB_MICROSOFT_TENANT
        // isn't interpolated into the endpoint URL.
        let tenant = match std::env::var("RTDB_MICROSOFT_TENANT") {
            Ok(v) if !v.trim().is_empty() => v,
            _ => "common".to_string(),
        };
        Self {
            client_id,
            client_secret,
            tenant,
        }
    }
}

/// Sign in with Apple. Apple rejects a static client_secret: the secret sent
/// to Apple's token endpoint is a short-lived ES256 JWT the server signs with
/// the private key registered with Apple, assembled from four config pieces.
/// Identity keys on Apple's stable `sub` (see `auth::apple`), because Apple
/// may relay the email through `@privaterelay.appleid.com`. RTDB_APPLE_* env.
#[derive(Clone, Debug, Default)]
pub struct AppleOAuth {
    pub client_id: Option<String>,   // RTDB_APPLE_CLIENT_ID (Services ID)
    pub team_id: Option<String>,     // RTDB_APPLE_TEAM_ID
    pub key_id: Option<String>,      // RTDB_APPLE_KEY_ID
    pub private_key: Option<String>, // RTDB_APPLE_PRIVATE_KEY (PEM, \n-escaped)
}

impl AppleOAuth {
    pub(super) fn from_env() -> Self {
        // Sign in with Apple. The private key is a PEM, which can't carry real
        // newlines through most env stores, so `\n` escapes are unescaped here.
        let client_id = std::env::var("RTDB_APPLE_CLIENT_ID").ok();
        let team_id = std::env::var("RTDB_APPLE_TEAM_ID").ok();
        let key_id = std::env::var("RTDB_APPLE_KEY_ID").ok();
        let private_key = std::env::var("RTDB_APPLE_PRIVATE_KEY")
            .ok()
            .map(|v| v.replace("\\n", "\n"));
        Self {
            client_id,
            team_id,
            key_id,
            private_key,
        }
    }
}

/// All six OAuth providers, nested under `Config::oauth`.
#[derive(Clone, Debug, Default)]
pub struct OAuthConfig {
    pub github: GithubOAuth,
    pub google: GoogleOAuth,
    pub gitlab: GitlabOAuth,
    pub oidc: OidcProvider,
    pub microsoft: MicrosoftOAuth,
    pub apple: AppleOAuth,
}

impl OAuthConfig {
    pub(super) fn from_env() -> Self {
        Self {
            github: GithubOAuth::from_env(),
            google: GoogleOAuth::from_env(),
            gitlab: GitlabOAuth::from_env(),
            oidc: OidcProvider::from_env(),
            microsoft: MicrosoftOAuth::from_env(),
            apple: AppleOAuth::from_env(),
        }
    }
}
