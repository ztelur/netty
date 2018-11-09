/**
 * Superid.menkor.com Inc.
 * Copyright (c) 2012-2018 All Rights Reserved.
 */
package io.netty.example.heartbeat;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

/**
 *
 * @author libing
 * @version $Id: HeartbeatHandler.java, v 0.1 2018年11月08日 下午4:24 zt Exp $
 */
public class HeartbeatHandler extends ChannelInboundHandlerAdapter {
    private int lossConnectCount = 0;
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        System.out.println("已经5秒未收到客户端的消息了！");

        if (evt instanceof IdleStateEvent) {
            IdleStateEvent event = (IdleStateEvent)evt;
            if (event.state() == IdleState.READER_IDLE) {
                lossConnectCount ++;
                if (lossConnectCount > 2) {
                    System.out.println("关闭这个不活跃的通道");
                    ctx.channel().close();
                }
            }
        } else {
            super.userEventTriggered(ctx, evt);
        }

    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        lossConnectCount = 0;
        System.out.println("client says:" + msg.toString());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        ctx.close();
    }
}