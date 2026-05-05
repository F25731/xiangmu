# Resource Transfer Service

自用资源抓取和网盘转存服务。

## 功能

- HTTP 抓取 `xiangmu.eu.cc` 资源列表
- 只提取夸克网盘和百度网盘链接
- 保留原始标题、简介、图片
- 支持首次抓取最近 N 条
- 定时扫描强制开启，后续自动发现新增资源
- 抓到资源后必须先转存到你的网盘，成功后才入库
- 有夸克就转夸克，有百度就转百度；任意一个成功就入库并可推送
- 某个网盘失败会按 provider 进入失败队列；后续成功后只补到同一条资源，不会二次推送
- 提供后台实时详情框，所有转存和报错都会记录
- 提供本地 API 给同服务器上的其他网站调用，并按程序标记已推送资源

## 本地启动

```powershell
cd C:\Users\F1589\Desktop\卡网\resource_service
python -m pip install -r requirements.txt
python -m uvicorn app:app --host 127.0.0.1 --port 8080
```

打开：

```text
http://127.0.0.1:8080/
```

## 你的其他网站调用

获取最新资源：

```text
GET http://127.0.0.1:8080/api/resources/latest?limit=20
```

返回里的 `new_links` 是转存后你自己的分享链接。主资源接口不会给其他程序返回原始网盘链接。

按程序获取待推送资源：

```text
GET http://127.0.0.1:8080/api/clients/my-site/pending?first_limit=20&limit=10
```

- `my-site` 换成你的程序标识
- 第一次调用会补 `first_limit` 条
- 后续只返回这个程序没拿过的新资源
- 默认返回后自动标记已推送

如果你想自己确认推送成功后再标记：

```text
GET http://127.0.0.1:8080/api/clients/my-site/pending?mark=false
```

推送成功后：

```bash
curl -X POST http://127.0.0.1:8080/api/clients/my-site/ack \
  -H 'Content-Type: application/json' \
  -d '{"ids":[1,2,3]}'
```

## Docker

把你的脚本放到：

```text
resource_service/scripts/quark_xinyue_test.py
resource_service/scripts/baidu_openlist_test.py
```

然后：

```bash
docker compose up -d --build
```

默认只监听服务器本机：

```text
127.0.0.1:8080
```

反代时把域名转到这个本地地址即可。

## 服务器部署

假设服务器已经安装 Docker 和 Docker Compose：

```bash
git clone https://github.com/F25731/xiangmu.git
cd xiangmu
docker compose up -d --build
```

查看运行状态：

```bash
docker compose ps
docker compose logs -f
```

控制页默认地址：

```text
http://服务器IP:8080/
```

如果你使用 Nginx 反代，建议容器仍然只监听本机 `127.0.0.1:8080`，公网只开放 Nginx。

示例：

```nginx
server {
    listen 80;
    server_name 你的域名;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

重载 Nginx：

```bash
sudo nginx -t
sudo systemctl reload nginx
```

如果你设置了 `APP_ADMIN_TOKEN`，访问控制页时可以带：

```text
https://你的域名/?token=你的长随机字符串
```

浏览器会把 token 存在本地，后续控制台 API 会自动带上。

## 更新部署

后面我推新版本后，服务器里执行：

```bash
cd xiangmu
git pull
docker compose up -d --build
```

数据在 `./data`，正常更新镜像不会丢。

## 安全

如果控制页要暴露到公网，建议设置 `APP_ADMIN_TOKEN`，再由反代限制访问来源或加 Basic Auth。

`docker-compose.yml` 里可以这样填：

```yaml
environment:
  APP_DB_PATH: /app/data/resource_service.db
  APP_ADMIN_TOKEN: "换成你的长随机字符串"
```

设置后，请求控制 API 需要带：

```text
X-Admin-Token: 换成你的长随机字符串
```

控制页目前适合内网或反代加鉴权后使用。
