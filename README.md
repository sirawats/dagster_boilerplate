# dagster_boilerplate

## Guide
1. Create conda environment
   ```
   conda create --prefix .venv python=3.11
   ```
2. Install `pdm`, which is Python package manager
   ```
   pip3 install pdm
   ```
3. Install dependencies for local development
   ```
   pdm install
   ```
    **[Additional]** When you need to add more dependencies, use `pdm add <package_name>`
4. Run docker-compose
   ```
   cd deployment
   docker-compose up -d
   ```