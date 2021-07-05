addEventListener("fetch", async (event) => {
    event.respondWith(handleRequest(event.request));
});

async function handleRequest(req) {
    _callServiceWrapper({
        Sync: {
            Detach: {
                timeout_ms: 10000,
                cpu_timeout_ms: 100,
            }
        }
    }, []);
    setTimeout(() => {
        console.log("Hello after 8 seconds!");
        _callServiceWrapper({
            Sync: "Done",
        }, []);
    }, 8000);
    return new Response("Done");
}
