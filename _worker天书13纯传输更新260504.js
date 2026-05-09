import { connect } from 'cloudflare:sockets';

//说明：Pages版本，只建议WS传输，不建议xhttp/stream-one
//部署路径：/functions/[[path]].js

let 哎呀呀这是我的VL密钥 = "5b75df69-62e0-4f8d-82f4-c4763c6a9ec3"; //建议改成自己的UUID

let 启用反代功能 = true;
let 反代IP = 'ProxyIP.JP.CMLiussss.net';

let 启用SOCKS5反代 = false;
let 启用SOCKS5全局反代 = false;

let 启用部署检测 = true; // 浏览器访问显示 Hello World

let 我的SOCKS5账号 = [
  '@Enkelte_notif:@Notif_Chat@115.91.26.114:2470',
];

////////////////////////////////////////////////////////////////////////// Pages入口 ////////////////////////////////////////////////////////////////////////

export async function onRequest(context) {

  const 访问请求 = context.request;

  const 读取路径 = decodeURIComponent(
    访问请求.url.replace(/^https?:\/\/[^/]+/, '')
  );

  const 取参数 = (key) =>
    读取路径.match(new RegExp(`(?:^|[/?&])${key}=([^&/]+)`))?.[1];

  const 解析布尔 = (值, 默认) =>
    ({ true: true, false: false }[值] ?? 默认);

  反代IP = 取参数('proxyip') || 反代IP;

  const SOCKS5新账号 = 取参数('socks5');

  我的SOCKS5账号 = [
    ...(SOCKS5新账号 ? [SOCKS5新账号] : []),
    ...我的SOCKS5账号
  ];

  启用SOCKS5反代 = 解析布尔(
    取参数('socks5-open'),
    启用SOCKS5反代
  );

  启用SOCKS5全局反代 = 解析布尔(
    取参数('socks5-global'),
    启用SOCKS5全局反代
  );

  // WebSocket
  if (访问请求.headers.get('Upgrade') === 'websocket') {

    const [客户端, WS接口] = Object.values(new WebSocketPair());

    WS接口.accept();
    WS接口.binaryType = "arraybuffer";

    处理数据(WS接口, true);

    return new Response(null, {
      status: 101,
      webSocket: 客户端
    });
  }

  // XHTTP
  else if (
    访问请求.method === 'POST' &&
    访问请求.body
  ) {

    return await 处理数据(访问请求, false);
  }

  // 部署成功检测
  if (启用部署检测) {
    return new Response('Hello World!', {
      status: 200,
      headers: {
        'content-type': 'text/plain; charset=utf-8'
      }
    });
  }

  return new Response(null, { status: 404 });
}

////////////////////////////////////////////////////////////////////////// 主要架构 ////////////////////////////////////////////////////////////////////////

async function 处理数据(数据接口, 传输协议, 发送数据, 传输队列 = Promise.resolve()) {

  if (传输协议) {
    处理WS流();
  } else {
    return await 处理XHTTP流();
  }

  async function 处理WS流(是首包 = true, 处理首包数据 = Promise.resolve()) {

    数据接口.addEventListener('message', async event => {

      await 传输队列;

      if (是首包) {

        是首包 = false;

        处理首包数据 = 处理首包数据
          .then(async () => await 处理首包(event.data))
          .catch(e => { throw e });

      } else {

        await 处理首包数据;

        传输队列 = 传输队列
          .then(async () => {
            try {
              await 发送数据.write(event.data);
            } catch {}
          })
          .catch(e => { throw e });
      }
    });

    async function 处理首包(首包数据) {

      const 解析首包 = await 解析首包数据(
        new Uint8Array(首包数据)
      );

      发送数据 = 解析首包.TCP接口.writable.getWriter();

      if (解析首包.是DNS) {
        数据接口.send(解析首包.初始数据);
        return;
      }

      await 发送数据.write(解析首包.初始数据);

      数据回传通道(
        解析首包.TCP接口,
        解析首包.版本号
      ).pipeTo(
        new WritableStream({
          write(数据) {
            数据接口.send(数据);
          }
        })
      );
    }
  }

  async function 处理XHTTP流() {

    const 读取器 = 数据接口.body.getReader();

    const 请求数据 = (await 读取器.read()).value;

    const 解析首包 = await 解析首包数据(
      new Uint8Array(请求数据)
    );

    if (解析首包.是DNS) {
      return new Response(解析首包.初始数据);
    }

    发送数据 = 解析首包.TCP接口.writable.getWriter();

    await 发送数据.write(解析首包.初始数据);

    数据发送通道(读取器);

    return new Response(
      数据回传通道(
        解析首包.TCP接口,
        解析首包.版本号
      )
    );

    async function 数据发送通道(读取器) {

      while (true) {

        await 传输队列;

        const {
          done: 流结束,
          value: 请求数据
        } = await 读取器.read();

        if (流结束) break;

        if (请求数据?.length > 0) {

          传输队列 = 传输队列
            .then(async () => {
              try {
                await 发送数据.write(请求数据);
              } catch {}
            })
            .catch(e => { throw e });
        }
      }
    }
  }

  function 数据回传通道(TCP接口, 版本号) {

    const 读取管道 = new TransformStream({

      async start(控制器) {
        控制器.enqueue(
          new Uint8Array([版本号, 0])
        );
      },

      transform(返回数据, 控制器) {

        传输队列 = 传输队列
          .then(() => 控制器.enqueue(返回数据))
          .catch(e => { throw e });
      }
    });

    TCP接口.readable.pipeTo(读取管道.writable);

    return 读取管道.readable;
  }
}

////////////////////////////////////////////////////////////////////////// 解析首包 ////////////////////////////////////////////////////////////////////////

async function 解析首包数据(二进制数据) {

  let 识别地址类型, 访问地址, 地址长度;

  if (二进制数据.length < 32) {
    throw new Error('数据长度不足');
  }

  const 获取协议头 = 二进制数据[0];

  const 验证VL的密钥 = (a, i = 0) =>
    [...a.slice(i, i + 16)]
      .map(b => b.toString(16).padStart(2, '0'))
      .join('')
      .replace(
        /(.{8})(.{4})(.{4})(.{4})(.{12})/,
        '$1-$2-$3-$4-$5'
      );

  if (
    验证VL的密钥(二进制数据.slice(1, 17))
    !== 哎呀呀这是我的VL密钥
  ) {
    throw new Error('UUID验证失败');
  }

  const 提取端口索引 =
    18 + 二进制数据[17] + 1;

  const 访问端口 =
    new DataView(
      二进制数据.buffer,
      提取端口索引,
      2
    ).getUint16(0);

  // DNS
  if (访问端口 === 53) {

    const 提取DNS查询报文 =
      二进制数据.slice(提取端口索引 + 9);

    const 查询DOH结果 = await fetch(
      'https://1.1.1.1/dns-query',
      {
        method: 'POST',
        headers: {
          'content-type': 'application/dns-message',
        },
        body: 提取DNS查询报文
      }
    );

    const 提取DOH结果 =
      await 查询DOH结果.arrayBuffer();

    const 构建长度头部 = new Uint8Array([
      (提取DOH结果.byteLength >> 8) & 0xff,
      提取DOH结果.byteLength & 0xff
    ]);

    const 拼接DNS结果 = await new Blob([
      构建长度头部,
      提取DOH结果
    ]);

    return {
      初始数据: 拼接DNS结果,
      是DNS: true
    };
  }

  const 提取地址索引 = 提取端口索引 + 2;

  识别地址类型 = 二进制数据[提取地址索引];

  let 地址信息索引 = 提取地址索引 + 1;

  switch (识别地址类型) {

    case 1:

      地址长度 = 4;

      访问地址 = 二进制数据
        .slice(地址信息索引, 地址信息索引 + 地址长度)
        .join('.');

      break;

    case 2:

      地址长度 = 二进制数据[地址信息索引];

      地址信息索引 += 1;

      访问地址 = new TextDecoder().decode(
        二进制数据.slice(
          地址信息索引,
          地址信息索引 + 地址长度
        )
      );

      break;

    case 3:

      地址长度 = 16;

      const ipv6 = [];

      const 读取IPV6地址 = new DataView(
        二进制数据.buffer,
        地址信息索引,
        16
      );

      for (let i = 0; i < 8; i++) {
        ipv6.push(
          读取IPV6地址
            .getUint16(i * 2)
            .toString(16)
            .padStart(4, '0')
        );
      }

      访问地址 = ipv6.join(':');

      break;

    default:
      throw new Error('无效的访问地址');
  }

  const 写入初始数据 =
    二进制数据.slice(地址信息索引 + 地址长度);

  const TCP接口 = await 创建TCP接口连接(
    访问地址,
    访问端口,
    识别地址类型
  );

  return {
    版本号: 获取协议头,
    TCP接口,
    初始数据: 写入初始数据
  };
}
