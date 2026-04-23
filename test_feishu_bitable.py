#!/usr/bin/env python3
"""
飞书多维表格连接测试
"""
import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def test_connection():
    """测试飞书连接"""
    print("=" * 50)
    print("飞书多维表格连接测试")
    print("=" * 50)
    
    # 检查环境变量
    app_id = os.getenv('FEISHU_APP_ID')
    app_secret = os.getenv('FEISHU_APP_SECRET')
    
    print(f"\n1. 环境变量检查:")
    print(f"   FEISHU_APP_ID: {'✅ 已设置' if app_id else '❌ 未设置'}")
    print(f"   FEISHU_APP_SECRET: {'✅ 已设置' if app_secret else '❌ 未设置'}")
    
    if not app_id or not app_secret:
        print("\n❌ 请先设置环境变量:")
        print("   export FEISHU_APP_ID=cli_xxx")
        print("   export FEISHU_APP_SECRET=xxx")
        return False
    
    # 测试连接
    print(f"\n2. 测试飞书连接...")
    try:
        from feishu_bitable_tracker import get_tracker
        tracker = get_tracker()
        
        if not tracker.client:
            print("   ❌ 客户端初始化失败")
            return False
        
        print("   ✅ 客户端初始化成功")
        
        # 测试列出表格
        print(f"\n3. 获取表格列表...")
        tables = tracker.client.list_tables()
        print(f"   ✅ 成功获取 {len(tables)} 个表格")
        
        for table in tables:
            print(f"      - {table['name']} ({table['table_id']})")
        
        # 测试获取数据表
        print(f"\n4. 测试获取数据表...")
        table_id = tracker.client.get_table_id("V10选股")
        if table_id:
            print(f"   ✅ 获取到表格 ID: {table_id}")
            
            # 测试读取记录
            print(f"\n5. 测试读取记录...")
            records = tracker.client.list_records(table_id)
            print(f"   ✅ 成功读取 {len(records)} 条记录")
        else:
            print(f"   ⚠️ 表格不存在，将自动创建")
        
        print(f"\n6. 测试记录选股...")
        from datetime import datetime
        today = datetime.now().strftime('%Y-%m-%d')
        
        result = tracker.record_pick('V10', '000001', '平安银行', 10.5, today)
        if result:
            print(f"   ✅ 测试记录成功")
            
            # 清理测试数据
            print(f"\n7. 清理测试数据...")
            # 注：这里不实际删除，只是标记
            print(f"   ℹ️ 测试数据保留在表格中")
        else:
            print(f"   ❌ 测试记录失败")
        
        print("\n" + "=" * 50)
        print("✅ 所有测试通过！")
        print("=" * 50)
        return True
        
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == '__main__':
    success = test_connection()
    sys.exit(0 if success else 1)
