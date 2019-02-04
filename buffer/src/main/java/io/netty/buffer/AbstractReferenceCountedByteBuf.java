/*
 * Copyright 2013 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.netty.buffer;

import io.netty.util.IllegalReferenceCountException;
import io.netty.util.internal.PlatformDependent;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static io.netty.util.internal.ObjectUtil.checkPositive;

/**
 * Abstract base class for {@link ByteBuf} implementations that count references.
 * 实现引用计数的获取和自身的增加和减少
 */
public abstract class AbstractReferenceCountedByteBuf extends AbstractByteBuf {
    private static final long REFCNT_FIELD_OFFSET;
    private static final AtomicIntegerFieldUpdater<AbstractReferenceCountedByteBuf> refCntUpdater =
            AtomicIntegerFieldUpdater.newUpdater(AbstractReferenceCountedByteBuf.class, "refCnt");
    /**
     * 计数器基于 AtomicIntegerFieldUpdater ，为什么不直接用 AtomicInteger ？因为 ByteBuf 对象很多，
     * 如果都把 int 包一层 AtomicInteger 花销较大，而AtomicIntegerFieldUpdater 只需要一个全局的静态变量。
     */
    private volatile int refCnt = 1;

    static {
        long refCntFieldOffset = -1;
        try {
            if (PlatformDependent.hasUnsafe()) {
                refCntFieldOffset = PlatformDependent.objectFieldOffset(
                        AbstractReferenceCountedByteBuf.class.getDeclaredField("refCnt"));
            }
        } catch (Throwable ignore) {
            refCntFieldOffset = -1;
        }

        REFCNT_FIELD_OFFSET = refCntFieldOffset;
    }

    protected AbstractReferenceCountedByteBuf(int maxCapacity) {
        super(maxCapacity);
    }

    @Override
    int internalRefCnt() {
        // Try to do non-volatile read for performance as the ensureAccessible() is racy anyway and only provide
        // a best-effort guard.
        //
        // TODO: Once we compile against later versions of Java we can replace the Unsafe usage here by varhandles.
        return REFCNT_FIELD_OFFSET != -1 ? PlatformDependent.getInt(this, REFCNT_FIELD_OFFSET) : refCnt();
    }

    @Override
    public int refCnt() {
        return refCnt;
    }

    /**
     * An unsafe operation intended for use by a subclass that sets the reference count of the buffer directly
     */
    protected final void setRefCnt(int refCnt) {
        refCntUpdater.set(this, refCnt);
    }

    @Override
    public ByteBuf retain() {
        return retain0(1);
    }

    @Override
    public ByteBuf retain(int increment) {
        return retain0(checkPositive(increment, "increment"));
    }

    private ByteBuf retain0(final int increment) {
        // 增加
        int oldRef = refCntUpdater.getAndAdd(this, increment);
        // 原有的ref为负数或者increment为负数
        if (oldRef <= 0 || oldRef + increment < oldRef) {
            // Ensure we don't resurrect (which means the refCnt was 0) and also that we encountered an overflow.
            refCntUpdater.getAndAdd(this, -increment);
            throw new IllegalReferenceCountException(oldRef, increment);
        }
        return this;
    }

    @Override
    public ByteBuf touch() {
        return this;
    }

    @Override
    public ByteBuf touch(Object hint) {
        return this;
    }

    @Override
    public boolean release() {
        return release0(1);
    }

    @Override
    public boolean release(int decrement) {
        return release0(checkPositive(decrement, "decrement"));
    }

    private boolean release0(int decrement) {
        // 减少
        int oldRef = refCntUpdater.getAndAdd(this, -decrement);
        // 相等，说明rfc已经是0了，所以进行释放
        if (oldRef == decrement) {
            deallocate();
            return true;
        }
        // 减少的值得大于 原有 oldRef ，说明“越界”；或者，increment 为负数
        if (oldRef < decrement || oldRef - decrement > oldRef) {
            // Ensure we don't over-release, and avoid underflow.
            refCntUpdater.getAndAdd(this, decrement);
            throw new IllegalReferenceCountException(oldRef, -decrement);
        }
        return false;
    }
    /**
     * Called once {@link #refCnt()} is equals 0.
     */
    protected abstract void deallocate();
}
