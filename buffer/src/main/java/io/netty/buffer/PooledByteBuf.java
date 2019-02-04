/*
 * Copyright 2012 The Netty Project
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

import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

abstract class PooledByteBuf<T> extends AbstractReferenceCountedByteBuf {
    /**
     * Recycler处理器，用于回收对象
     */
    private final Recycler.Handle<PooledByteBuf<T>> recyclerHandle;
    /**
     * Chunk对象
     */
    protected PoolChunk<T> chunk;
    /**
     * 从chunk对象中分配的内存块所处的位置
     */
    protected long handle;
    /**
     * 内存空间，具体什么样子通过泛型来设置
     */
    protected T memory;
    /**
     * 开始位置
     */
    protected int offset;
    /**
     * 容量
     */
    protected int length;
    /**
     * 占用memory的大小
     */
    int maxLength;
    PoolThreadCache cache;
    /**
     * 临时的ByteBuf对象
     */
    private ByteBuffer tmpNioBuf;
    /**
     * 分配器
     */
    private ByteBufAllocator allocator;

    @SuppressWarnings("unchecked")
    protected PooledByteBuf(Recycler.Handle<? extends PooledByteBuf<T>> recyclerHandle, int maxCapacity) {
        super(maxCapacity);
        this.recyclerHandle = (Handle<PooledByteBuf<T>>) recyclerHandle;
    }

    void init(PoolChunk<T> chunk, long handle, int offset, int length, int maxLength, PoolThreadCache cache) {
        init0(chunk, handle, offset, length, maxLength, cache);
    }

    void initUnpooled(PoolChunk<T> chunk, int length) {
        init0(chunk, 0, chunk.offset, length, length, null);
    }

    /**
     * 初始化方法
     * @param chunk
     * @param handle
     * @param offset
     * @param length
     * @param maxLength
     * @param cache
     */
    private void init0(PoolChunk<T> chunk, long handle, int offset, int length, int maxLength, PoolThreadCache cache) {
        assert handle >= 0;
        assert chunk != null;

        this.chunk = chunk;
        memory = chunk.memory;
        allocator = chunk.arena.parent;
        this.cache = cache;
        this.handle = handle;
        this.offset = offset;
        this.length = length;
        this.maxLength = maxLength;
        tmpNioBuf = null;
    }
    /**
     * 每次重用时，都必须调用该函数
     */
    final void reuse(int maxCapacity) {
        // 设置最大容量
        maxCapacity(maxCapacity);
        // 设置引用数为1
        setRefCnt(1);
        // 重置读写索引为0
        setIndex0(0, 0);
        discardMarks();
    }

    /**
     * maxLength是占用memory的最大容量，而maxCapacity才是真正的最大容量，当写入量超过maxLength时
     * 会进行扩容，知道maxCapacity
     * @return
     */
    @Override
    public final int capacity() {
        return length;
    }

    @Override
    public final ByteBuf capacity(int newCapacity) {
        // 校验新的容量，不能超过最大容量
        checkNewCapacity(newCapacity);

        // If the request capacity does not require reallocation, just update the length of the memory.
        // 如果chunk是非池化的，当容量相等，无需扩容或者缩小，直接返回
        if (chunk.unpooled) {
            if (newCapacity == length) {
                return this;
            }
        } else {
            // chunk是池化的
            // 扩容
            if (newCapacity > length) {
                // 小于memory的最大length所以可以不作处理(只需要设置length)，直接返回
                if (newCapacity <= maxLength) {
                    length = newCapacity;
                    return this;
                }
            // 缩容
            } else if (newCapacity < length) {
                // 大于maxLength的一半，否则就不进行缩容，因为会浪费很多内存。
                if (newCapacity > maxLength >>> 1) {

                    if (maxLength <= 512) {
                        if (newCapacity > maxLength - 16) {
                            length = newCapacity;
                            setIndex(Math.min(readerIndex(), newCapacity), Math.min(writerIndex(), newCapacity));
                            return this;
                        }
                    } else { // > 512 (i.e. >= 1024)
                        length = newCapacity;
                        setIndex(Math.min(readerIndex(), newCapacity), Math.min(writerIndex(), newCapacity));
                        return this;
                    }
                }
            } else {
                // 相等，直接返回
                return this;
            }
        }
        // 不满足上述条件，重新分配新的内存空间，并将数据复制到其中，并且，释放老的内存空间。
        // Reallocation required.
        chunk.arena.reallocate(this, newCapacity, true);
        return this;
    }

    @Override
    public final ByteBufAllocator alloc() {
        return allocator;
    }

    @Override
    public final ByteOrder order() {
        return ByteOrder.BIG_ENDIAN;
    }

    @Override
    public final ByteBuf unwrap() {
        return null;
    }

    @Override
    public final ByteBuf retainedDuplicate() {
        return PooledDuplicatedByteBuf.newInstance(this, this, readerIndex(), writerIndex());
    }

    @Override
    public final ByteBuf retainedSlice() {
        final int index = readerIndex();
        return retainedSlice(index, writerIndex() - index);
    }

    @Override
    public final ByteBuf retainedSlice(int index, int length) {
        return PooledSlicedByteBuf.newInstance(this, this, index, length);
    }

    protected final ByteBuffer internalNioBuffer() {
        ByteBuffer tmpNioBuf = this.tmpNioBuf;
        if (tmpNioBuf == null) {
            this.tmpNioBuf = tmpNioBuf = newInternalNioBuffer(memory);
        }
        return tmpNioBuf;
    }

    protected abstract ByteBuffer newInternalNioBuffer(T memory);

    /**
     *  当引用计数为0时，调用该函数，进行内存回收
     */
    @Override
    protected final void deallocate() {
        if (handle >= 0) {
            final long handle = this.handle;
            this.handle = -1;
            memory = null;
            tmpNioBuf = null;
            // 释放内存回 Arena 中
            chunk.arena.free(chunk, handle, maxLength, cache);
            chunk = null;
            recycle();
        }
    }

    private void recycle() {
        recyclerHandle.recycle(this);
    }

    protected final int idx(int index) {
        return offset + index;
    }
}
