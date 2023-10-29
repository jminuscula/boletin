from aiohttp import web


async def status(request):
    status_code = request.match_info["code"]
    content = request.content or "response body"
    kwargs = {"status": status_code, "body": request.content, "headers": {"x-ratelimit-reset-requests": "10"}}
    return web.Response(**kwargs)


if __name__ == "__main__":
    app = web.Application()
    app.add_routes([web.get("/status/{code}", status)])
    web.run_app(app)
