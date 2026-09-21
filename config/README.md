# Runtime configuration

Runtime configuration is read from environment variables with the `QSA_` prefix. The repository-level `.env.example` documents supported values.

The application never writes secrets to this directory. Local secrets belong in an ignored `.env` file or an external secret manager.
