# Set up Vault environment variables
export VAULT_ADDR='http://192.168.3.38:8200'
export VAULT_TOKEN='test'

# Retrieve secrets from Vault
screener_username=$(vault kv get -field=username secret/myapp)
screener_password=$(vault kv get -field=password secret/myapp)

# Print retrieved secrets
echo "Retrieved Screener Username: $screener_username"
echo "Retrieved Screener Password: $screener_password"

