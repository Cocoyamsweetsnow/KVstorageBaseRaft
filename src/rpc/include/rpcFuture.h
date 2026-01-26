#ifndef RPCFUTURE_H
#define RPCFUTURE_H

#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <string>

#include <google/protobuf/message.h>

namespace rpc
{

enum class RpcStatus
{
    PENDING,  // 等待中
    SUCCESS,  // 成功
    FAILED,   // 失败
    TIMEOUT,  // 超时
    CANCELLED // 已取消
};

class RpcResult
{
public:
    RpcResult() : status_(RpcStatus::PENDING) {}

    RpcResult(RpcStatus status, const std::string &errorMsg = "") : status_(status), errorMsg_(errorMsg) {}

    bool isSuccess() const { return status_ == RpcStatus::SUCCESS; }
    bool isFailed() const { return status_ == RpcStatus::FAILED; }
    bool isTimeout() const { return status_ == RpcStatus::TIMEOUT; }
    bool isCancelled() const { return status_ == RpcStatus::CANCELLED; }
    bool isPending() const { return status_ == RpcStatus::PENDING; }

    RpcStatus getStatus() const { return status_; }
    const std::string &getErrorMsg() const { return errorMsg_; }

    void setStatus(RpcStatus status) { status_ = status; }
    void setErrorMsg(const std::string &msg) { errorMsg_ = msg; }

private:
    RpcStatus status_;
    std::string errorMsg_;
};

template <typename ResponseType> class RpcFuture
{
public:
    using Callback = std::function<void(const RpcResult &, const ResponseType *)>;
    using ptr = std::shared_ptr<RpcFuture<ResponseType>>;

    RpcFuture() : response_(nullptr), ready_(false) {}

    
    RpcResult wait()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this]() { return ready_; });
        return result_;
    }

    
    RpcResult waitFor(int64_t timeoutMs)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        if (cv_.wait_for(lock, std::chrono::milliseconds(timeoutMs), [this]() { return ready_; }))
        {
            return result_;
        }
        return RpcResult(RpcStatus::TIMEOUT, "RPC call timeout");
    }

   
    bool isReady() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return ready_;
    }

    ResponseType *getResponse()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return response_;
    }

   
    RpcResult getResult()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return result_;
    }

   
    void setCallback(Callback cb)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        callback_ = std::move(cb);
        // 如果已经完成，立即调用回调
        if (ready_ && callback_)
        {
            callback_(result_, response_);
        }
    }

   
    void cancel()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!ready_)
        {
            result_ = RpcResult(RpcStatus::CANCELLED, "RPC call cancelled");
            ready_ = true;
            cv_.notify_all();
            if (callback_)
            {
                callback_(result_, nullptr);
            }
        }
    }

    // 以下方法供Channel内部使用
    void setResult(RpcStatus status, const std::string &errorMsg = "")
    {
        std::lock_guard<std::mutex> lock(mutex_);
        result_ = RpcResult(status, errorMsg);
        ready_ = true;
        cv_.notify_all();
        if (callback_)
        {
            callback_(result_, response_);
        }
    }

    void setResponse(ResponseType *response)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        response_ = response;
    }

    void complete(RpcStatus status, ResponseType *response, const std::string &errorMsg = "")
    {
        std::lock_guard<std::mutex> lock(mutex_);
        result_ = RpcResult(status, errorMsg);
        response_ = response;
        ready_ = true;
        cv_.notify_all();
        if (callback_)
        {
            callback_(result_, response_);
        }
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    RpcResult result_;
    ResponseType *response_;
    bool ready_;
    Callback callback_;
};


class GenericRpcFuture
{
public:
    using Callback = std::function<void(const RpcResult &, google::protobuf::Message *)>;
    using ptr = std::shared_ptr<GenericRpcFuture>;

    GenericRpcFuture() : response_(nullptr), ready_(false) {}

    RpcResult wait()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this]() { return ready_; });
        return result_;
    }

    RpcResult waitFor(int64_t timeoutMs)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        if (cv_.wait_for(lock, std::chrono::milliseconds(timeoutMs), [this]() { return ready_; }))
        {
            return result_;
        }
        return RpcResult(RpcStatus::TIMEOUT, "RPC call timeout");
    }

    bool isReady() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return ready_;
    }

    google::protobuf::Message *getResponse()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return response_;
    }

    RpcResult getResult()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return result_;
    }

    void setCallback(Callback cb)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        callback_ = std::move(cb);
        if (ready_ && callback_)
        {
            callback_(result_, response_);
        }
    }

    void cancel()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!ready_)
        {
            result_ = RpcResult(RpcStatus::CANCELLED, "RPC call cancelled");
            ready_ = true;
            cv_.notify_all();
            if (callback_)
            {
                callback_(result_, nullptr);
            }
        }
    }

    void complete(RpcStatus status, google::protobuf::Message *response, const std::string &errorMsg = "")
    {
        std::lock_guard<std::mutex> lock(mutex_);
        result_ = RpcResult(status, errorMsg);
        response_ = response;
        ready_ = true;
        cv_.notify_all();
        if (callback_)
        {
            callback_(result_, response_);
        }
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    RpcResult result_;
    google::protobuf::Message *response_;
    bool ready_;
    Callback callback_;
};

} // namespace rpc

#endif 
