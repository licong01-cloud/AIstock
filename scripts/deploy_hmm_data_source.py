"""
部署配置脚本

初始化 HMM 数据源所需的数据库权限和目录结构。

运行方式:
    python scripts/deploy_hmm_data_source.py
"""

import asyncio
from pathlib import Path

from backend.db.pg_pool import get_conn


async def setup_database_permissions():
    """
    设置数据库权限

    创建只读用户和读写用户，并授予适当权限。
    """
    print("📊 Setting up database permissions...")

    async with get_conn() as conn:
        async with conn.cursor() as cur:
            # 1. 创建只读用户（如果不存在）
            print("  - Creating hmm_evolution_ro user...")
            await cur.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'hmm_evolution_ro') THEN
                        CREATE USER hmm_evolution_ro WITH PASSWORD 'change_me_in_production';
                    END IF;
                END
                $$;
            """)

            # 2. 授予 market.* 表的 SELECT 权限
            print("  - Granting SELECT on market.* tables...")
            await cur.execute("""
                GRANT SELECT ON market.kline_daily_raw TO hmm_evolution_ro;
                GRANT SELECT ON market.sw_member TO hmm_evolution_ro;
                GRANT SELECT ON market.trade_cal TO hmm_evolution_ro;
                GRANT SELECT ON market.stock_basic TO hmm_evolution_ro;
            """)

            # 3. 创建读写用户（继承只读权限）
            print("  - Creating hmm_evolution_rw user...")
            await cur.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'hmm_evolution_rw') THEN
                        CREATE USER hmm_evolution_rw WITH PASSWORD 'change_me_in_production';
                    END IF;
                END
                $$;
            """)

            # 继承只读权限
            await cur.execute("""
                GRANT hmm_evolution_ro TO hmm_evolution_rw;
            """)

            # 4. 创建演进系统 schema（如果不存在）
            print("  - Creating hmm_evolution schema...")
            await cur.execute("""
                CREATE SCHEMA IF NOT EXISTS hmm_evolution;
            """)

            await cur.execute("""
                CREATE SCHEMA IF NOT EXISTS hmm_risk;
            """)

            # 5. 授予读写权限
            print("  - Granting ALL on hmm_evolution.* and hmm_risk.*...")
            await cur.execute("""
                GRANT ALL ON SCHEMA hmm_evolution TO hmm_evolution_rw;
                GRANT ALL ON SCHEMA hmm_risk TO hmm_evolution_rw;
                GRANT ALL ON ALL TABLES IN SCHEMA hmm_evolution TO hmm_evolution_rw;
                GRANT ALL ON ALL TABLES IN SCHEMA hmm_risk TO hmm_evolution_rw;
                ALTER DEFAULT PRIVILEGES IN SCHEMA hmm_evolution GRANT ALL ON TABLES TO hmm_evolution_rw;
                ALTER DEFAULT PRIVILEGES IN SCHEMA hmm_risk GRANT ALL ON TABLES TO hmm_evolution_rw;
            """)

    print("✅ Database permissions configured successfully")


def setup_directory_structure():
    """
    创建目录结构

    创建缓存目录和日志目录。
    """
    print("📁 Setting up directory structure...")

    directories = [
        "tmp/hmm_evolution_cache",
        "logs/hmm_evolution",
    ]

    for dir_path in directories:
        path = Path(dir_path)
        path.mkdir(parents=True, exist_ok=True)
        print(f"  - Created: {dir_path}")

    print("✅ Directory structure created")


def setup_gitignore():
    """
    更新 .gitignore

    确保缓存目录不被 git 跟踪。
    """
    print("📝 Updating .gitignore...")

    gitignore_path = Path(".gitignore")

    entries_to_add = [
        "# HMM Evolution Cache",
        "tmp/hmm_evolution_cache/",
        "logs/hmm_evolution/",
    ]

    if gitignore_path.exists():
        content = gitignore_path.read_text()
    else:
        content = ""

    for entry in entries_to_add:
        if entry not in content:
            content += f"\n{entry}"

    gitignore_path.write_text(content)

    print("✅ .gitignore updated")


async def verify_installation():
    """
    验证安装

    检查权限、目录和依赖。
    """
    print("🔍 Verifying installation...")

    # 1. 检查目录
    assert Path("tmp/hmm_evolution_cache").exists(), "Cache directory not found"
    print("  ✓ Cache directory exists")

    # 2. 检查数据库连接
    async with get_conn() as conn:
        async with conn.cursor() as cur:
            # 检查 schema
            await cur.execute("""
                SELECT schema_name FROM information_schema.schemata
                WHERE schema_name IN ('hmm_evolution', 'hmm_risk')
            """)
            schemas = await cur.fetchall()
            assert len(schemas) == 2, "Schemas not created"
            print("  ✓ Database schemas exist")

            # 检查用户
            await cur.execute("""
                SELECT rolname FROM pg_roles
                WHERE rolname IN ('hmm_evolution_ro', 'hmm_evolution_rw')
            """)
            users = await cur.fetchall()
            assert len(users) == 2, "Users not created"
            print("  ✓ Database users exist")

    # 3. 检查 Python 依赖
    import importlib.util

    missing = [
        name for name in ("pandas", "pydantic")
        if importlib.util.find_spec(name) is None
    ]
    if missing:
        raise ImportError(f"Missing required dependencies: {', '.join(missing)}")
    print("  ✓ Python dependencies installed")

    print("✅ Installation verified successfully")


async def main():
    """主函数"""
    print("=" * 60)
    print("HMM Data Source Deployment Script")
    print("=" * 60)
    print()

    try:
        # 1. 设置目录结构
        setup_directory_structure()
        print()

        # 2. 更新 .gitignore
        setup_gitignore()
        print()

        # 3. 设置数据库权限
        await setup_database_permissions()
        print()

        # 4. 验证安装
        await verify_installation()
        print()

        print("=" * 60)
        print("✅ Deployment completed successfully!")
        print("=" * 60)
        print()
        print("Next steps:")
        print("  1. Update database passwords in production")
        print("  2. Run tests: pytest tests/backend/services/hmm_data_source/")
        print("  3. Proceed to Phase 1 implementation")

    except Exception as e:
        print()
        print("=" * 60)
        print(f"❌ Deployment failed: {e}")
        print("=" * 60)
        raise


if __name__ == "__main__":
    asyncio.run(main())
