from aiohttp import web


async def status(request):
    status_code = request.match_info["code"]
    content = request.content or "response body"
    kwargs = {
        "status": status_code,
        "body": request.content,
        "headers": {"1x-ratelimit-reset-requests": "10"},
    }
    return web.Response(**kwargs)


should_error = True


async def status_alternate(request):
    global should_error

    if should_error:
        res = web.Response(status=500, body="error")
    else:
        res = web.Response(status=200, body="ok")

    should_error = not should_error
    return res


if __name__ == "__main__":
    app = web.Application()
    app.add_routes([web.get("/status/alternate", status_alternate)])
    app.add_routes([web.get("/status/{code}", status)])
    web.run_app(app)
