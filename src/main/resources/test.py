import asyncio
import httpx


# 异步函数：发送单个HTTP请求
async def fetch_url(client: httpx.AsyncClient, url: str) -> tuple:
    """
    发送异步GET请求，返回请求结果元组（URL, 状态码, 响应内容/错误信息）
    """
    try:
        # 发送GET请求，设置超时时间10秒
        response = await client.get(url, timeout=10.0)
        # 返回URL、状态码、响应文本（简化处理，实际可按需提取数据）
        return (url, response.status_code, response.text[:200])  # 仅取前200字符
    except Exception as e:
        # 捕获异常（如超时、网络错误等）
        return (url, None, f"请求失败：{str(e)}")


# 主异步函数：批量发起请求并等待结果
async def main():
    # 要请求的URL列表（可替换为实际需要的地址）
    urls = [
        "https://www.baidu.com",
        "https://www.github.com",
        "https://www.python.org",
        "https://httpbin.org/get",  # 用于测试的API
        "https://example.com",
    ]

    # 创建异步HTTP客户端（可设置公共参数，如headers、代理等）
    async with httpx.AsyncClient() as client:
        # 批量创建任务：为每个URL生成一个fetch_url协程
        tasks = [fetch_url(client, url) for url in urls]

        print("开始发送异步请求...")
        # 等待所有任务完成（并发执行），返回结果列表（顺序与tasks对应）
        results = await asyncio.gather(*tasks)

    # 处理所有请求结果
    print("\n===== 所有请求完成，结果如下 =====")
    for url, status_code, content in results:
        if status_code == 200:
            print(f"✅ {url}")
            print(f"   状态码：{status_code}")
            print(f"   响应预览：{content[:100]}...\n")  # 仅展示前100字符
        else:
            print(f"❌ {url}")
            print(f"   错误信息：{content}\n")


# 运行主异步函数（Python 3.7+ 可用）
if __name__ == "__main__":
    # 启动事件循环，执行main函数并等待完成
    asyncio.run(main())