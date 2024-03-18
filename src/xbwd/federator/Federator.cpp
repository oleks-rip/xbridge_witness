//------------------------------------------------------------------------------
/*
    This file is part of rippled: https://github.com/ripple/rippled
    Copyright (c) 2021 Ripple Labs Inc.

    Permission to use, copy, modify, and/or distribute this software for any
    purpose  with  or without fee is hereby granted, provided that the above
    copyright notice and this permission notice appear in all copies.

    THE  SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
    WITH  REGARD  TO  THIS  SOFTWARE  INCLUDING  ALL  IMPLIED  WARRANTIES  OF
    MERCHANTABILITY  AND  FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
    ANY  SPECIAL ,  DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
    WHATSOEVER  RESULTING  FROM  LOSS  OF USE, DATA OR PROFITS, WHETHER IN AN
    ACTION  OF  CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
    OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
*/
//==============================================================================

#include <xbwd/federator/Federator.h>

#include <xbwd/app/App.h>
#include <xbwd/app/DBInit.h>
#include <xbwd/basics/ChainTypes.h>
#include <xbwd/basics/StructuredLog.h>
#include <xbwd/client/RpcResultParse.h>
#include <xbwd/federator/TxnSupport.h>

#include <ripple/basics/strHex.h>
#include <ripple/beast/core/CurrentThreadName.h>
#include <ripple/json/Output.h>
#include <ripple/json/json_reader.h>
#include <ripple/json/json_writer.h>
#include <ripple/protocol/AccountID.h>
#include <ripple/protocol/SField.h>
#include <ripple/protocol/STParsedJSON.h>
#include <ripple/protocol/STTx.h>
#include <ripple/protocol/Seed.h>
#include <ripple/protocol/jss.h>

#include <fmt/core.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <exception>
#include <filesystem>
#include <fstream>
#include <future>
#include <sstream>
#include <stdexcept>

namespace xbwd {

std::shared_ptr<Federator>
make_Federator(
    App& app,
    boost::asio::io_service& ios,
    config::Config const& config,
    ripple::Logs& l)
{
    auto r = std::make_shared<Federator>(
        Federator::PrivateTag{}, app, config, l.journal("Federator"));
    r->init(ios, config, l);

    return r;
}

Federator::Chain::Chain(config::ChainConfig const& config)
    : rewardAccount_{config.rewardAccount}
    , txnSubmit_(config.txnSubmit)
    , lastAttestedCommitTx_(config.lastAttestedCommitTx)
{
}

Federator::Federator(
    PrivateTag,
    App& app,
    config::Config const& config,
    beast::Journal j)
    : app_{app}
    , bridge_{config.bridge}
    , dataDir(config.dataDir.string())
    , chains_{Chain{config.lockingChainConfig}, Chain{config.issuingChainConfig}}
    , autoSubmit_{chains_[ChainType::locking].txnSubmit_ &&
                  chains_[ChainType::locking].txnSubmit_->shouldSubmit,
                  chains_[ChainType::issuing].txnSubmit_ &&
                  chains_[ChainType::issuing].txnSubmit_->shouldSubmit}
    , maxAttToSend_(config.maxAttToSend)
    , signingAccount_(config.signingAccount)
    , keyType_{config.keyType}
    , signingPK_{derivePublicKey(config.keyType, config.signingKey)}
    , signingSK_{config.signingKey}
    , j_(j)
    , useBatch_(config.useBatch)
{
    signerListsInfo_[ChainType::locking].ignoreSignerList_ =
        config.lockingChainConfig.ignoreSignerList;
    signerListsInfo_[ChainType::issuing].ignoreSignerList_ =
        config.issuingChainConfig.ignoreSignerList;

    std::fill(loopLocked_.begin(), loopLocked_.end(), true);
    events_[ChainType::locking].reserve(16);
    events_[ChainType::issuing].reserve(16);
}

void
Federator::init(
    boost::asio::io_service& ios,
    config::Config const& config,
    ripple::Logs& l)
{
    std::filesystem::path fp(dataDir);
    fp /= "settings.json";
    std::fstream df(fp);

    if (df)
    {
        std::stringstream ss;
        ss << df.rdbuf();
        std::string const s = ss.str();
        if (!s.empty())
        {
            Json::Value jv;
            Json::Reader().parse(s, jv);
            std::uint32_t const lastLockingLedger =
                jv["LockingChain"]["processedLedger"].asUInt();
            initSync_[ChainType::locking].dbLedgerSqn_ = lastLockingLedger;
            std::uint32_t const lastIssuingLedger =
                jv["IssuingChain"]["processedLedger"].asUInt();
            initSync_[ChainType::issuing].dbLedgerSqn_ = lastIssuingLedger;
        }
    }

    for (auto const ct : {ChainType::locking, ChainType::issuing})
    {
        JLOGV(
            j_.info(),
            "Prepare init sync",
            jv("chainType", to_string(ct)),
            jv("DB ledgerSqn", initSync_[ct].dbLedgerSqn_),
            jv("DB txHash", initSync_[ct].dbTxnHash_),
            jv("config txHash",
               chains_[ct].lastAttestedCommitTx_
                   ? to_string(*chains_[ct].lastAttestedCommitTx_)
                   : std::string("not set")));
    }

    auto getSubmitAccount =
        [&](ChainType chainType) -> std::optional<ripple::AccountID> {
        auto const& chainConfig = chainType == ChainType::locking
            ? config.lockingChainConfig
            : config.issuingChainConfig;
        if (chainConfig.txnSubmit && chainConfig.txnSubmit->shouldSubmit)
        {
            return chainConfig.txnSubmit->submittingAccount;
        }
        return {};
    };

    std::shared_ptr<ChainListener> mainchainListener =
        std::make_shared<ChainListener>(
            ChainType::locking,
            config.bridge,
            getSubmitAccount(ChainType::locking),
            shared_from_this(),
            config.signingAccount,
            config.txLimit,
            initSync_[ChainType::locking].dbLedgerSqn_,
            l.journal("LListener"));

    std::shared_ptr<ChainListener> sidechainListener =
        std::make_shared<ChainListener>(
            ChainType::issuing,
            config.bridge,
            getSubmitAccount(ChainType::issuing),
            shared_from_this(),
            config.signingAccount,
            config.txLimit,
            initSync_[ChainType::issuing].dbLedgerSqn_,
            l.journal("IListener"));

    chains_[ChainType::locking].listener_ = std::move(mainchainListener);
    chains_[ChainType::locking].listener_->init(
        ios, config.lockingChainConfig.chainIp);
    chains_[ChainType::issuing].listener_ = std::move(sidechainListener);
    chains_[ChainType::issuing].listener_->init(
        ios, config.issuingChainConfig.chainIp);
}

Federator::~Federator()
{
    assert(!running_);
}

void
Federator::start()
{
    if (running_)
        return;
    requestStop_ = false;
    running_ = true;

    threads_[lt_event_locking] = std::thread([this]() {
        beast::setCurrentThreadName("FederatorEvents L");
        this->mainLoop(ChainType::locking);
    });
    threads_[lt_event_issuing] = std::thread([this]() {
        beast::setCurrentThreadName("FederatorEvents I");
        this->mainLoop(ChainType::issuing);
    });

    threads_[lt_txnSubmit] = std::thread([this]() {
        beast::setCurrentThreadName("FederatorTxns");
        this->txnSubmitLoop();
    });
}

void
Federator::stop()
{
    chains_[ChainType::locking].listener_->shutdown();
    chains_[ChainType::issuing].listener_->shutdown();

    if (running_)
    {
        requestStop_ = true;
        for (int i = 0; i < lt_last; ++i)
        {
            std::lock_guard l(cvMutexes_[i]);
            cvs_[i].notify_all();
        }

        for (int i = 0; i < lt_last; ++i)
            if (threads_[i].joinable())
                threads_[i].join();
        running_ = false;
    }
}

void
Federator::push(FederatorEvent&& e)
{
    ChainType ct;
    std::visit([&ct](auto const& e) { ct = e.chainType_; }, e);
    LoopTypes const lt =
        ct == ChainType::locking ? lt_event_locking : lt_event_issuing;

    bool notify = false;
    {
        std::lock_guard l{eventsMutex_};
        notify = events_[ct].empty();
        events_[ct].push_back(std::move(e));
    }
    if (notify)
    {
        std::lock_guard l(cvMutexes_[lt]);
        cvs_[lt].notify_all();
    }
}

// Called from 2 events that require attestations
void
Federator::initSync(
    ChainType const ct,
    ripple::uint256 const& eHash,
    std::int32_t const rpcOrder,
    FederatorEvent const& e)
{
    JLOG(j_.trace()) << "initSync start";

    if (!initSync_[ct].historyDone_)
    {
        if (initSync_[ct].dbTxnHash_ == eHash ||
            (chains_[ct].lastAttestedCommitTx_ == eHash))
        {
            initSync_[ct].historyDone_ = true;
            initSync_[ct].rpcOrder_ = rpcOrder;
            JLOGV(
                j_.info(),
                "initSync found previous tx",
                jv("event", toJson(e)));
        }
    }

    bool const historical = rpcOrder < 0;
    bool const skip = historical && initSync_[ct].historyDone_;
    if (!skip)
    {
        {
            // TODO remove after tests or when adding multiple bridges
            // assert order of insertion, so that the replay later will be in
            // order
            static ChainArray<std::int32_t> rpcOrderNew{-1, -1};
            static ChainArray<std::int32_t> rpcOrderOld{0, 0};
            JLOG(j_.trace()) << "initSync " << to_string(ct)
                             << ", rpcOrderNew=" << rpcOrderNew[ct]
                             << " rpcOrderOld=" << rpcOrderOld[ct]
                             << " rpcOrder=" << rpcOrder;
            if (historical)
            {
                assert(rpcOrderOld[ct] > rpcOrder);
                rpcOrderOld[ct] = rpcOrder;
            }
            else
            {
                assert(rpcOrderNew[ct] < rpcOrder);
                rpcOrderNew[ct] = rpcOrder;
            }
        }
        if (historical)
            replays_[ct].emplace_front(e);
        else
            replays_[ct].emplace_back(e);
    }

    tryFinishInitSync(ct);
}

std::optional<AttestedHistoryTx>
AttestedHistoryTx::fromEvent(FederatorEvent const& r)
{
    if (event::XChainCommitDetected const* pcd =
            std::get_if<event::XChainCommitDetected>(&r))
        return AttestedHistoryTx{
            xbwd::XChainTxnType::xChainAddClaimAttestation,
            pcd->src_,
            pcd->otherChainDst_ ? *pcd->otherChainDst_ : ripple::AccountID(),
            std::nullopt,
            pcd->claimID_};
    else if (
        event::XChainAccountCreateCommitDetected const* pac =
            std::get_if<event::XChainAccountCreateCommitDetected>(&r))
        return AttestedHistoryTx{
            xbwd::XChainTxnType::xChainAddAccountCreateAttestation,
            pac->src_,
            pac->otherChainDst_,
            pac->createCount_,
            std::nullopt};
    else
        return {};
}

void
Federator::tryFinishInitSync(ChainType const ct)
{
    ChainType const oct = otherChain(ct);

    if (!initSync_[ct].historyDone_)
        return;

    if (initSync_[ct].syncing_)
    {
        JLOGV(
            j_.info(),
            "initSyncDone",
            jv("chainType", to_string(ct)),
            jv("account", toBase58(bridge_.door(ct))),
            jv("events to replay", replays_[ct].size()));

        initSync_[ct].syncing_ = false;
        chains_[ct].listener_->stopHistoricalTxns();

        if (initSync_[oct].syncing_)
        {
            JLOGV(
                j_.info(),
                "initSyncDone waiting for other chain",
                jv("other chain", to_string(oct)));
        }
    }

    if (initSync_[oct].syncing_)
        return;

    for (auto const cht : {ChainType::locking, ChainType::issuing})
    {
        auto const ocht = otherChain(cht);
        auto& repl(replays_[cht]);
        std::uint32_t del_cnt = 0;
        std::size_t const rel_size = repl.size();
        for (auto it = repl.begin(); it != repl.end();)
        {
            auto const ah = AttestedHistoryTx::fromEvent(*it);
            // events from the one side checking against attestations from the
            // other side
            if (ah && initSync_[ocht].attestedTx_.contains(*ah))
            {
                ++del_cnt;
                it = repl.erase(it);
            }
            else
                ++it;
        }

        JLOGV(
            j_.info(),
            "initSyncDone start replay",
            jv("chainType", to_string(cht)),
            jv("account", ripple::toBase58(bridge_.door(cht))),
            jv("events to replay", rel_size),
            jv("attested events", initSync_[cht].attestedTx_.size()),
            jv("events to delete", del_cnt));

        for (auto const& event : repl)
        {
            std::visit([this](auto const& e) { this->onEvent(e); }, event);
        }
        repl.clear();
        initSync_[cht].attestedTx_.clear();
    }
}

bool
Federator::isSyncing() const
{
    return initSync_[ChainType::locking].syncing_ ||
        initSync_[ChainType::issuing].syncing_;
}

void
Federator::onEvent(event::XChainCommitDetected const& e)
{
    auto const ct = e.chainType_;
    auto const oct = otherChain(ct);
    JLOGV(j_.debug(), "onEvent XChainCommitDetected", jv("event", e.toJson()));

    if (isSyncing())
    {
        if (!e.rpcOrder_)
        {
            JLOGV(j_.error(), "syncing, no rpc order", jv("event", e.toJson()));
            return;
        }
        initSync(ct, e.txnHash_, *e.rpcOrder_, e);
        return;
    }

    if (e.rpcOrder_ && *e.rpcOrder_ < initSync_[ct].rpcOrder_)
    {
        JLOGV(
            j_.info(),
            "skip older",
            jv("event", e.toJson()),
            jv("initSync rpcOrder", initSync_[ct].rpcOrder_));
        return;
    }

    auto const txnIdHex = ripple::strHex(e.txnHash_.begin(), e.txnHash_.end());

    // soci complains about a bool
    int const success = ripple::isTesSuccess(e.status_) ? 1 : 0;
    // The attestation will be send from the other chain, so the other chain
    // will get the reward
    auto const& rewardAccount = chains_[oct].rewardAccount_;
    auto const& optDst = e.otherChainDst_;

    // non-const so it may be moved from
    auto claimOpt =
        [&]() -> std::optional<ripple::Attestations::AttestationClaim> {
        if (!success)
            return std::nullopt;
        if (!e.deliveredAmt_)
        {
            JLOGV(
                j_.error(),
                "missing delivered amount in successful xchain transfer",
                jv("event", e.toJson()));
            return std::nullopt;
        }

        return ripple::Attestations::AttestationClaim{
            e.bridge_,
            signingAccount_ ? *signingAccount_
                            : ripple::calcAccountID(signingPK_),
            signingPK_,
            signingSK_,
            e.src_,
            *e.deliveredAmt_,
            rewardAccount,
            ct == ChainType::locking,
            e.claimID_,
            optDst};
    }();

    assert(!claimOpt || claimOpt->verify(e.bridge_));

    // The attestation will be created by the other chain
    if (autoSubmit_[oct] && claimOpt)
    {
        bool processNow = e.ledgerBoundary_ || !e.rpcOrder_;
        pushAtt(e.bridge_, std::move(*claimOpt), oct, processNow);
    }
}

void
Federator::onEvent(event::XChainAccountCreateCommitDetected const& e)
{
    auto const ct = e.chainType_;
    auto const oct = otherChain(ct);
    JLOGV(
        j_.debug(),
        "onEvent XChainAccountCreateCommitDetected",
        jv("event", e.toJson()));

    if (isSyncing())
    {
        if (!e.rpcOrder_)
        {
            JLOGV(j_.error(), "no rpc order", jv("event", e.toJson()));
            return;
        }
        initSync(ct, e.txnHash_, *e.rpcOrder_, e);
        return;
    }

    if (e.rpcOrder_ && *e.rpcOrder_ < initSync_[ct].rpcOrder_)
    {
        JLOGV(
            j_.info(),
            "skip older",
            jv("event", e.toJson()),
            jv("initSync rpcOrder", initSync_[ct].rpcOrder_));
        return;
    }

    auto const txnIdHex = ripple::strHex(e.txnHash_.begin(), e.txnHash_.end());

    // soci complains about a bool
    int const success = ripple::isTesSuccess(e.status_) ? 1 : 0;
    // Attestation will be send from other chain, so the other chain will get
    // the reward
    auto const& rewardAccount = chains_[oct].rewardAccount_;
    auto const& dst = e.otherChainDst_;

    // non-const so it may be moved from
    auto createOpt =
        [&]() -> std::optional<ripple::Attestations::AttestationCreateAccount> {
        if (!success)
            return std::nullopt;
        if (!e.deliveredAmt_)
        {
            JLOGV(
                j_.error(),
                "missing delivered amount in successful xchain create transfer",
                jv("event", e.toJson()));
            return std::nullopt;
        }

        return ripple::Attestations::AttestationCreateAccount{
            e.bridge_,
            signingAccount_ ? *signingAccount_
                            : ripple::calcAccountID(signingPK_),
            signingPK_,
            signingSK_,
            e.src_,
            *e.deliveredAmt_,
            e.rewardAmt_,
            rewardAccount,
            ct == ChainType::locking,
            e.createCount_,
            dst};
    }();

    assert(!createOpt || createOpt->verify(e.bridge_));

    // The attestation will be created by the other chain
    if (autoSubmit_[oct] && createOpt)
    {
        bool processNow = e.ledgerBoundary_ || !e.rpcOrder_;
        pushAtt(e.bridge_, std::move(*createOpt), oct, processNow);
    }
}

void
Federator::onEvent(event::XChainTransferResult const& e)
{
    // TODO: Update the database with result info
    // really need this?
}

void
Federator::onEvent(event::HeartbeatTimer const& e)
{
    JLOGV(j_.trace(), "onEvent HeartbeatTimer", jv("event", e.toJson()));
}

void
Federator::onEvent(event::EndOfHistory const& e)
{
    JLOGV(j_.debug(), "onEvent EndOfHistory", jv("event", e.toJson()));
    auto const ct = e.chainType_;
    if (initSync_[ct].syncing_)
    {
        initSync_[ct].historyDone_ = true;
        tryFinishInitSync(ct);
    }
}

#ifdef USE_BATCH_ATTESTATION

std::pair<std::string, std::string>
forAttestIDs(
    ripple::STXChainAttestationBatch const& batch,
    std::function<void(std::uint64_t id)> commitFunc = [](std::uint64_t) {},
    std::function<void(std::uint64_t id)> createFunc = [](std::uint64_t) {})
{
    std::stringstream commitAttests;
    std::stringstream createAttests;
    auto temp = ripple::STXChainAttestationBatch::for_each_claim_batch<int>(
        batch.claims().begin(),
        batch.claims().end(),
        [&](auto batchStart, auto batchEnd) -> int {
            commitAttests << std::hex;
            for (auto i = batchStart; i != batchEnd; ++i)
            {
                commitAttests << ":" << i->claimID;
                commitFunc(i->claimID);
            }
            return 0;
        });

    temp = ripple::STXChainAttestationBatch::for_each_create_batch<int>(
        batch.creates().begin(),
        batch.creates().end(),
        [&](auto batchStart, auto batchEnd) -> int {
            createAttests << std::hex;
            for (auto i = batchStart; i != batchEnd; ++i)
            {
                createAttests << ":" << i->createCount;
                createFunc(i->createCount);
            }
            return 0;
        });

    return {commitAttests.str(), createAttests.str()};
}

#endif

std::pair<std::string, std::string>
forAttestIDs(
    ripple::Attestations::AttestationClaim const& claim,
    std::function<void(std::uint64_t id)> commitFunc = [](std::uint64_t) {},
    std::function<void(std::uint64_t id)> createFunc = [](std::uint64_t) {})
{
    std::stringstream commitAttests;

    auto const& claimID(claim.claimID);
    commitAttests << ":" << std::hex << claimID;
    commitFunc(claimID);

    return {commitAttests.str(), std::string()};
}

std::pair<std::string, std::string>
forAttestIDs(
    ripple::Attestations::AttestationCreateAccount const& create,
    std::function<void(std::uint64_t id)> commitFunc = [](std::uint64_t) {},
    std::function<void(std::uint64_t id)> createFunc = [](std::uint64_t) {})
{
    std::stringstream createAttests;

    auto const& createCount(create.createCount);
    createAttests << ":" << std::hex << createCount;
    createFunc(createCount);

    return {std::string(), createAttests.str()};
}

static std::unordered_set<ripple::TERUnderlyingType> SkippableTxnResult(
    {ripple::tesSUCCESS,
     ripple::tecXCHAIN_NO_CLAIM_ID,
     ripple::tecXCHAIN_SENDING_ACCOUNT_MISMATCH,
     ripple::tecXCHAIN_ACCOUNT_CREATE_PAST,
     ripple::tecXCHAIN_WRONG_CHAIN,
     ripple::tecXCHAIN_PROOF_UNKNOWN_KEY,
     ripple::tecXCHAIN_NO_SIGNERS_LIST,
     ripple::tecXCHAIN_BAD_TRANSFER_ISSUE,
     ripple::tecINSUFFICIENT_RESERVE,
     ripple::tecNO_DST_INSUF_XRP});

void
Federator::onEvent(event::XChainAttestsResult const& e)
{
    auto const ct = e.chainType_;
    auto const oct = otherChain(ct);

    JLOGV(j_.debug(), "onEvent XChainAttestsResult", jv("event", e.toJson()));

    if (!autoSubmit_[ct])
        return;

    // will resubmit after txn ttl (i.e. TxnTTLLedgers = 4) ledgers
    // may also get here during init sync.
    if (!SkippableTxnResult.contains(TERtoInt(e.ter_)))
        return;

    // Can be several attestations with the same ClaimID
    std::vector<SubmissionPtr> subToDelete;
    subToDelete.reserve(2);  // no more expected
    {
        std::lock_guard l{txnsMutex_};
        auto& subs = submitted_[ct];
        std::list<SubmissionPtr>::iterator it = subs.begin();
        do
        {
            if (it = std::find_if(
                    it,
                    subs.end(),
                    [&](auto const& i) {
                        return e.isFinal_
                            ? i->checkID(e.claimID_, e.createCount_)
                            : i->accountSqn_ == e.accountSqn_;
                    });
                it != subs.end())
            {
                subToDelete.push_back(std::move(*it));
                it = subs.erase(it);
            }
        } while (e.isFinal_ && (it != subs.end()));
    }

    for (auto& sub : subToDelete)
    {
        // Deleting events from the opposite side of the attestations
        auto const attestedIDs = sub->forAttestIDs(
            [&](std::uint64_t id) {}, [&](std::uint64_t id) {});
        JLOGV(
            j_.trace(),
            "XChainAttestsResult processed",
            jv("chainType", to_string(ct)),
            jv("accountSqn", e.accountSqn_),
            jv("result", e.ter_),
            jv("commitAttests", attestedIDs.first),
            jv("createAttests", attestedIDs.second));
    }

    if (e.isHistory_)
    {
        // save latest attestation
        initSync_[ct].attestedTx_.insert(AttestedHistoryTx{
            e.type_, e.src_, e.dst_, e.createCount_, e.claimID_});

        JLOGV(
            j_.debug(),
            "XChainAttestsResult add attestation",
            jv("chainType", to_string(ct)),
            jv("type", to_string(e.type_)),
            jv("src", e.src_),
            jv("dst", e.dst_),
            jv("createCount",
               e.createCount_ ? fmt::format("{:x}", *e.createCount_)
                              : std::string("0")),
            jv("claimID",
               e.claimID_ ? fmt::format("{:x}", *e.claimID_)
                          : std::string("0")));

        // tryFinishInitSync
        // This check can be put in any event processing function. But most
        // likely for the destination chain (for which this check was created),
        // it will fire here
        checkProcessedLedger(ct);
    }
}

void
Federator::onEvent(event::NewLedger const& e)
{
    auto const ct = e.chainType_;
    std::uint32_t const doorLedgerIndex =
        chains_[ct].listener_->getDoorProcessedLedger();
    std::uint32_t const submitLedgerIndex =
        chains_[ct].listener_->getSubmitProcessedLedger();

    JLOGV(
        j_.debug(),
        "onEvent NewLedger",
        jv("processedDoorLedger", doorLedgerIndex),
        jv("processedSubmitLedger", submitLedgerIndex),
        jv("event", e.toJson()));

    // Fix TTL for tx from DB
    [[maybe_unused]] static bool runOnce = [&]() {
        std::lock_guard l{txnsMutex_};
        auto& subs = submitted_[ct];
        for (auto& s : subs)
            if (!s->lastLedgerSeq_)
                s->lastLedgerSeq_ =
                    chains_[ct].listener_->getCurrentLedger() + TxnTTLLedgers;
        return true;
    }();

    // tryFinishInitSync
    checkProcessedLedger(ct);

    if (!autoSubmit_[ct] || isSyncing())
        return;

    auto const x =
        std::min(std::min(submitLedgerIndex, doorLedgerIndex), e.ledgerIndex_);
    auto const minLedger = x ? x - 1 : 0;
    saveProcessedLedger(ct, minLedger);
    checkExpired(ct, minLedger);
}

void
Federator::checkExpired(ChainType ct, std::uint32_t ledger)
{
    bool notify = false;
    {
        std::lock_guard l{txnsMutex_};

        auto& subs = submitted_[ct];
        auto& errs = errored_[ct];

        // add expired txn to errored_ for resubmit
        auto firstFresh =
            std::find_if(subs.begin(), subs.end(), [ledger](auto const& s) {
                return s->lastLedgerSeq_ > ledger;
            });

        for (auto it = subs.begin(); it != firstFresh;)
        {
            assert(!initSync_[ct].syncing_);
            auto& front = *it;
            if (front->retriesAllowed_ > 0)
            {
                JLOGV(
                    j_.warn(),
                    "Ledger TTL expired, move to errored",
                    jv("chainType", to_string(ct)),
                    jv("retries",
                       static_cast<std::uint32_t>(front->retriesAllowed_)),
                    jv("accSqn", front->accountSqn_),
                    jv("lastLedger", front->lastLedgerSeq_),
                    jv("tx", front->getJson()));

                front->retriesAllowed_--;
                front->accountSqn_ = 0;
                front->lastLedgerSeq_ = 0;
                ++it;
            }
            else
            {
                auto const attestedIDs = front->forAttestIDs();
                JLOGV(
                    j_.warn(),
                    "Giving up after repeated retries",
                    jv("chainType", to_string(ct)),
                    jv("commitAttests", attestedIDs.first),
                    jv("createAttests", attestedIDs.second),
                    jv(front->getLogName(), front->getJson()));
                it = subs.erase(it);
            }
        }

        if (subs.begin() != firstFresh)
        {
            auto& front = subs.front();
            auto where =
                std::find_if(errs.begin(), errs.end(), [&front](auto const& s) {
                    return *front < *s;
                });
            errs.insert(
                where,
                std::make_move_iterator(subs.begin()),
                std::make_move_iterator(firstFresh));
            subs.erase(subs.begin(), firstFresh);
        }

        notify = !errored_[ct].empty();
    }
    if (notify)
    {
        std::lock_guard l(cvMutexes_[lt_txnSubmit]);
        cvs_[lt_txnSubmit].notify_all();
    }
    else
    {
        std::lock_guard bl{batchMutex_};
        if (curClaimAtts_[ct].size() + curCreateAtts_[ct].size() > 0)
            pushAttOnSubmitTxn(bridge_, ct);
    }
}

void
Federator::checkProcessedLedger(ChainType ct)
{
    auto const historyProcessedLedger =
        chains_[ct].listener_->getHistoryProcessedLedger();
    // If last tx not set (expected for issuing side) then check last processed
    // ledger
    if (initSync_[ct].syncing_ && !initSync_[ct].historyDone_ &&
        initSync_[ct].dbLedgerSqn_ && historyProcessedLedger &&
        (historyProcessedLedger <= initSync_[ct].dbLedgerSqn_))
    {
        initSync_[ct].historyDone_ = true;
        JLOGV(
            j_.trace(),
            "initSync found previous processed ledger",
            jv("chain", to_string(ct)),
            jv("dbLedger", initSync_[ct].dbLedgerSqn_),
            jv("historyLedger", historyProcessedLedger));
        tryFinishInitSync(ct);
    }
}

void
Federator::saveProcessedLedger(ChainType ct, std::uint32_t ledger)
{
    if (ledger > initSync_[ct].dbLedgerSqn_)
    {
        initSync_[ct].dbLedgerSqn_ = ledger;

        std::filesystem::path fp(dataDir);
        fp /= "settings.json";
        std::fstream df(fp, std::ios_base::out | std::ios::trunc);

        if (df)
        {
            Json::Value j;
            j["LockingChain"] = Json::objectValue;
            j["IssuingChain"] = Json::objectValue;
            j["LockingChain"]["processedLedger"] =
                initSync_[ChainType::locking].dbLedgerSqn_;
            j["IssuingChain"]["processedLedger"] =
                initSync_[ChainType::issuing].dbLedgerSqn_;
            std::string const s = j.toStyledString();
            df << s << std::endl;

            JLOGV(
                j_.trace(),
                "syncDB update processed ledger",
                jv("chainType", to_string(ct)),
                jv("ledger", ledger));
        }
        else
        {
            throw std::runtime_error(fmt::format("Can't open {}", fp.string()));
        }
    }
}

void
Federator::updateSignerListStatus(ChainType const chainType)
{
    auto const signingAcc =
        signingAccount_ ? *signingAccount_ : calcAccountID(signingPK_);
    auto& signerListInfo(signerListsInfo_[chainType]);

    // check signer list
    signerListInfo.status_ = signerListInfo.presentInSignerList_
        ? SignerListInfo::present
        : SignerListInfo::absent;

    // check master key
    if ((signerListInfo.status_ != SignerListInfo::present) &&
        !signerListInfo.disableMaster_)
    {
        auto const& masterDoorID = bridge_.door(chainType);
        if (masterDoorID == signingAcc)
            signerListInfo.status_ = SignerListInfo::present;
    }

    // check regular key
    if ((signerListInfo.status_ != SignerListInfo::present) &&
        signerListInfo.regularDoorID_.isNonZero() &&
        (signerListInfo.regularDoorID_ == signingAcc))
    {
        signerListInfo.status_ = SignerListInfo::present;
    }
}

void
Federator::onEvent(event::XChainSignerListSet const& e)
{
    auto const signingAcc =
        signingAccount_ ? *signingAccount_ : calcAccountID(signingPK_);
    auto& signerListInfo(signerListsInfo_[e.chainType_]);

    signerListInfo.presentInSignerList_ =
        e.signerList_.find(signingAcc) != e.signerList_.end();
    updateSignerListStatus(e.chainType_);

    JLOGV(
        j_.info(),
        "onEvent XChainSignerListSet",
        jv("SigningAcc", ripple::toBase58(signingAcc)),
        jv("DoorID", ripple::toBase58(e.masterDoorID_)),
        jv("ChainType", to_string(e.chainType_)),
        jv("SignerListInfo", signerListInfo.toJson()));
}

void
Federator::onEvent(event::XChainSetRegularKey const& e)
{
    auto const signingAcc =
        signingAccount_ ? *signingAccount_ : calcAccountID(signingPK_);
    auto& signerListInfo(signerListsInfo_[e.chainType_]);

    signerListInfo.regularDoorID_ = e.regularDoorID_;
    updateSignerListStatus(e.chainType_);

    JLOGV(
        j_.info(),
        "onEvent XChainSetRegularKey",
        jv("SigningAcc", ripple::toBase58(signingAcc)),
        jv("DoorID", ripple::toBase58(e.masterDoorID_)),
        jv("ChainType", to_string(e.chainType_)),
        jv("SignerListInfo", signerListInfo.toJson()));
}

void
Federator::onEvent(event::XChainAccountSet const& e)
{
    auto const signingAcc =
        signingAccount_ ? *signingAccount_ : calcAccountID(signingPK_);
    auto& signerListInfo(signerListsInfo_[e.chainType_]);

    signerListInfo.disableMaster_ = e.disableMaster_;
    updateSignerListStatus(e.chainType_);

    JLOGV(
        j_.info(),
        "onEvent XChainAccountSet",
        jv("SigningAcc", ripple::toBase58(signingAcc)),
        jv("DoorID", ripple::toBase58(e.masterDoorID_)),
        jv("ChainType", to_string(e.chainType_)),
        jv("SignerListInfo", signerListInfo.toJson()));
}

void
Federator::pushAttOnSubmitTxn(
    ripple::STXChainBridge const& bridge,
    ChainType chainType)
{
    // batch mutex must already be held
    bool notify = false;
    auto const& signerListInfo(signerListsInfo_[chainType]);
    if (signerListInfo.ignoreSignerList_ ||
        (signerListInfo.status_ != SignerListInfo::absent))
    {
        JLOGV(
            j_.debug(),
            "In the signer list, the attestations are proceed.",
            jv("ChainType", to_string(chainType)));

        std::lock_guard tl{txnsMutex_};
        notify = txns_[ChainType::locking].empty() &&
            txns_[ChainType::issuing].empty();

        if (useBatch_)
        {
#ifdef USE_BATCH_ATTESTATION
            auto p = SubmissionPtr(new SubmissionBatch(
                0,
                0,
                ripple::STXChainAttestationBatch{
                    bridge,
                    curClaimAtts_[chainType].begin(),
                    curClaimAtts_[chainType].end(),
                    curCreateAtts_[chainType].begin(),
                    curCreateAtts_[chainType].end()}));

            txns_[chainType].emplace_back(std::move(p));
#else
            throw std::runtime_error(
                "Please compile with USE_BATCH_ATTESTATION to use Batch "
                "Attestations");
#endif
        }
        else
        {
            // lastLedgerSeq, accountSqn will be updated right before sending
            for (auto const& claim : curClaimAtts_[chainType])
            {
                auto p = SubmissionPtr(new SubmissionClaim(
                    0, 0, networkID_[chainType], bridge, claim));
                txns_[chainType].emplace_back(std::move(p));
            }

            for (auto const& create : curCreateAtts_[chainType])
            {
                auto p = SubmissionPtr(new SubmissionCreateAccount(
                    0, 0, networkID_[chainType], bridge, create));
                txns_[chainType].emplace_back(std::move(p));
            }
        }
    }
    else
    {
        JLOGV(
            j_.info(),
            "Not in the signer list, the attestations has been dropped.",
            jv("ChainType", to_string(chainType)));
    }

    curClaimAtts_[chainType].clear();
    curCreateAtts_[chainType].clear();

    if (notify)
    {
        std::lock_guard l(cvMutexes_[lt_txnSubmit]);
        cvs_[lt_txnSubmit].notify_all();
    }
}

void
Federator::pushAtt(
    ripple::STXChainBridge const& bridge,
    ripple::Attestations::AttestationClaim&& att,
    ChainType chainType,
    bool ledgerBoundary)
{
    std::lock_guard bl{batchMutex_};
    curClaimAtts_[chainType].emplace_back(std::move(att));
    auto const attSize =
        curClaimAtts_[chainType].size() + curCreateAtts_[chainType].size();
    assert(attSize <= maxAttests());
    if (ledgerBoundary || attSize >= maxAttests())
        pushAttOnSubmitTxn(bridge, chainType);
}

void
Federator::pushAtt(
    ripple::STXChainBridge const& bridge,
    ripple::Attestations::AttestationCreateAccount&& att,
    ChainType chainType,
    bool ledgerBoundary)
{
    std::lock_guard bl{batchMutex_};
    curCreateAtts_[chainType].emplace_back(std::move(att));
    auto const attSize =
        curClaimAtts_[chainType].size() + curCreateAtts_[chainType].size();
    assert(attSize <= maxAttests());
    if (ledgerBoundary || attSize >= maxAttests())
        pushAttOnSubmitTxn(bridge, chainType);
}

void
Federator::submitTxn(SubmissionPtr&& submission, ChainType ct)
{
    if (submission->numAttestations() == 0)
        return;

    // already verified txnSubmit before call submitTxn()
    config::TxnSubmit const& txnSubmit = *chains_[ct].txnSubmit_;
    ripple::XRPAmount fee{
        chains_[ct].listener_->getCurrentFee() + FeeExtraDrops};
    ripple::STTx const toSubmit = submission->getSignedTxn(txnSubmit, fee, j_);

    auto const attestedIDs = submission->forAttestIDs();
    JLOGV(
        j_.trace(),
        "Submitting transaction",
        jv("chainType", to_string(ct)),
        jv("commitAttests", attestedIDs.first),
        jv("createAttests", attestedIDs.second),
        jv("lastLedgerSeq", submission->lastLedgerSeq_),
        jv("accountSqn", submission->accountSqn_),
        jv("hash",
           to_string(toSubmit.getHash(ripple::HashPrefix::transactionID))),
        jv(submission->getLogName(), submission->getJson()));

    Json::Value const request = [&] {
        Json::Value r;
        r[ripple::jss::tx_blob] =
            ripple::strHex(toSubmit.getSerializer().peekData());
        return r;
    }();

    auto callback = [this, ct](Json::Value const& v) {
        // drop tem submissions. Other errors will be processed after txn TTL.
        if (v.isMember(ripple::jss::result))
        {
            auto const& result = v[ripple::jss::result];
            if (result.isMember(ripple::jss::engine_result_code) &&
                result[ripple::jss::engine_result_code].isIntegral())
            {
                auto txnTER = ripple::TER::fromInt(
                    result[ripple::jss::engine_result_code].asInt());
                if (ripple::isTemMalformed(txnTER))
                {
                    if (result.isMember(ripple::jss::tx_json))
                    {
                        auto const& txJson = result[ripple::jss::tx_json];
                        if (txJson.isMember(ripple::jss::Sequence) &&
                            txJson[ripple::jss::Sequence].isIntegral())
                        {
                            std::uint32_t sqn =
                                txJson[ripple::jss::Sequence].asUInt();

                            std::lock_guard l{txnsMutex_};
                            auto& subs = submitted_[ct];
                            if (auto it = std::find_if(
                                    subs.begin(),
                                    subs.end(),
                                    [&](auto const& i) {
                                        return i->accountSqn_ == sqn;
                                    });
                                it != subs.end())
                            {
                                auto const attestedIDs = (*it)->forAttestIDs();
                                JLOGV(
                                    j_.warn(),
                                    "Tem txn submit result, removing "
                                    "submission",
                                    jv("account sequence", sqn),
                                    jv("chainType", to_string(ct)),
                                    jv("commitAttests", attestedIDs.first),
                                    jv("createAttests", attestedIDs.second));
                                subs.erase(it);
                            }
                        }
                    }
                }
            }
        }
    };

    {
        std::lock_guard tl{txnsMutex_};
        submitted_[ct].emplace_back(std::move(submission));
    }
    chains_[ct].listener_->send("submit", request, callback);
    // JLOG(j_.trace()) << "txn submitted";  // the listener logs as well
}

void
Federator::unlockMainLoop()
{
    for (int i = 0; i < lt_last; ++i)
    {
        std::lock_guard l(loopMutexes_[i]);
        loopLocked_[i] = false;
        loopCvs_[i].notify_all();
    }
}

void
Federator::mainLoop(ChainType ct)
{
    LoopTypes const lt =
        ct == ChainType::locking ? lt_event_locking : lt_event_issuing;
    {
        std::unique_lock l{loopMutexes_[lt]};
        loopCvs_[lt].wait(l, [this, lt] { return !loopLocked_[lt]; });
    }

    auto& events(events_[ct]);
    std::vector<FederatorEvent> localEvents;
    localEvents.reserve(16);
    while (!requestStop_)
    {
        {
            std::lock_guard l{eventsMutex_};
            assert(localEvents.empty());
            localEvents.swap(events);
        }
        if (localEvents.empty())
        {
            using namespace std::chrono_literals;
            // In rare cases, an event may be pushed and the condition
            // variable signaled before the condition variable is waited on.
            // To handle this, set a timeout on the wait.
            std::unique_lock l{cvMutexes_[lt]};
            // Allow for spurious wakeups. The alternative requires locking the
            // eventsMutex_
            cvs_[lt].wait_for(l, 1s);
            continue;
        }

        for (auto const& event : localEvents)
            std::visit([this](auto&& e) { this->onEvent(e); }, event);
        localEvents.clear();
    }
}

void
Federator::txnSubmitLoop()
{
    ChainArray<std::string> accountStrs;
    for (ChainType ct : {ChainType::locking, ChainType::issuing})
    {
        config::TxnSubmit const& txnSubmit = *chains_[ct].txnSubmit_;
        if (chains_[ct].txnSubmit_ && chains_[ct].txnSubmit_->shouldSubmit)
            accountStrs[ct] =
                ripple::toBase58(chains_[ct].txnSubmit_->submittingAccount);
        else
            JLOG(j_.warn())
                << "Will not submit transaction for chain " << to_string(ct);
    }
    if (accountStrs[ChainType::locking].empty() &&
        accountStrs[ChainType::issuing].empty())
    {
        JLOG(j_.error()) << "Will not submit transaction for ANY chain ";
        return;
    }

    auto const lt = lt_txnSubmit;
    {
        std::unique_lock l{loopMutexes_[lt]};
        loopCvs_[lt].wait(l, [this, lt] { return !loopLocked_[lt]; });
    }

    ChainArray<bool> waitingAccountInfo{false, false};
    ChainArray<std::uint32_t> accountInfoSqns{0u, 0u};
    std::mutex accountInfoMutex;

    // Return true if ready to submit txn.
    // Lifetime of the captured variables is almost program-wide, cause 'while
    // (!requestStop_)' loop will never stop. The loop will stop just before
    // shutdown and connections will be closed too, so callback will not fire.
    auto getReady = [&](ChainType chain) -> bool {
        if (!chains_[chain].listener_->getCurrentLedger() ||
            !chains_[chain].listener_->getCurrentFee())
        {
            JLOG(j_.trace())
                << "Not ready, waiting for validated ledgers from stream";
            return false;
        }

        // TODO add other readiness check such as verify if witness is in
        // signerList as needed

        if (accountSqns_[chain] != 0)
            return true;

        {
            std::lock_guard aiLock{accountInfoMutex};
            if (waitingAccountInfo[chain])
                return false;

            if (accountInfoSqns[chain] != 0)
            {
                accountSqns_[chain] = accountInfoSqns[chain];
                accountInfoSqns[chain] = 0;
                return true;
            }
            waitingAccountInfo[chain] = true;
        }

        auto callback = [&, ct = chain](Json::Value const& accountInfo) {
            if (accountInfo.isMember(ripple::jss::result) &&
                accountInfo[ripple::jss::result].isMember("account_data"))
            {
                auto const& ad =
                    accountInfo[ripple::jss::result]["account_data"];
                if (ad.isMember(ripple::jss::Sequence) &&
                    ad[ripple::jss::Sequence].isIntegral())
                {
                    std::lock_guard aiLock{accountInfoMutex};
                    assert(waitingAccountInfo[ct] && accountInfoSqns[ct] == 0);
                    accountInfoSqns[ct] = ad[ripple::jss::Sequence].asUInt();
                    waitingAccountInfo[ct] = false;
                    JLOG(j_.trace())
                        << "got account sqn " << accountInfoSqns[ct];
                }
            }
        };
        Json::Value request;
        request[ripple::jss::account] = accountStrs[chain];
        request[ripple::jss::ledger_index] = "validated";
        chains_[chain].listener_->send("account_info", request, callback);
        JLOG(j_.trace()) << "Not ready, waiting account sqn";
        return false;
    };

    std::uint32_t skipCtr = 0;
    while (!requestStop_)
    {
        bool waitForEvent = true;

        for (auto const ct : {ChainType::locking, ChainType::issuing})
        {
            if (accountStrs[ct].empty())
                continue;

            decltype(txns_)::type localTxns;
            decltype(txns_)::type* pLocal = nullptr;
            bool checkReady = false;

            {
                std::lock_guard l{txnsMutex_};
                if (maxAttToSend_ && (submitted_[ct].size() > maxAttToSend_))
                {
                    ++skipCtr;
                    continue;
                }

                if (errored_[ct].empty())
                {
                    if (!txns_[ct].empty())
                    {
                        checkReady = true;
                        pLocal = &(txns_[ct]);
                    }
                }
                else
                {
                    if (submitted_[ct].empty())
                    {
                        accountSqns_[ct] = 0;
                        checkReady = true;
                        pLocal = &(errored_[ct]);
                    }
                }
            }

            if (checkReady)
            {
                if (!getReady(ct))
                    continue;
                std::lock_guard l{txnsMutex_};

                std::uint32_t const numToSend = maxAttToSend_
                    ? (submitted_[ct].size() <= maxAttToSend_
                           ? maxAttToSend_ - submitted_[ct].size()
                           : 0)
                    : pLocal->size();
                if (!numToSend)
                {
                    ++skipCtr;
                    continue;
                }

                if (pLocal->size() <= numToSend)
                    localTxns.swap(*pLocal);
                else
                {
                    JLOGV(
                        j_.trace(),
                        "Waiting size exceed window size",
                        jv("chainType", to_string(ct)),
                        jv("waiting size", pLocal->size()),
                        jv("window size", maxAttToSend_),
                        jv("send size", numToSend),
                        jv("skipped iterations", skipCtr));
                    skipCtr = 0;

                    auto start = pLocal->begin();
                    auto finish = start + numToSend;
                    localTxns.assign(
                        std::make_move_iterator(start),
                        std::make_move_iterator(finish));
                    pLocal->erase(start, finish);
                }
            }

            waitForEvent = waitForEvent && localTxns.empty();

            for (auto& txn : localTxns)
            {
                auto const lastLedgerSeq =
                    chains_[ct].listener_->getCurrentLedger() + TxnTTLLedgers;
                txn->lastLedgerSeq_ = lastLedgerSeq;
                txn->accountSqn_ = accountSqns_[ct]++;
                submitTxn(std::move(txn), ct);
            }
        }

        if (waitForEvent)
        {
            using namespace std::chrono_literals;
            // In rare cases, an event may be pushed and the condition
            // variable signaled before the condition variable is waited on.
            // To handle this, set a timeout on the wait.
            std::unique_lock l{cvMutexes_[lt]};
            // Allow for spurious wakeups. The alternative requires locking the
            // eventsMutex_
            cvs_[lt].wait_for(l, 1s);
            continue;
        }
    }
}

Json::Value
Federator::getInfo() const
{
    // TODO
    // Track transactions per seconds
    // Track when last transaction or event was submitted
    Json::Value ret{Json::objectValue};
    {
        // Pending events
        // In most cases, events have been moved by event loop thread
        std::lock_guard l{eventsMutex_};
        auto const sz = events_[ChainType::locking].size() +
            events_[ChainType::issuing].size();
        ret["pending_events_size"] = (int)sz;
        if (sz > 0)
        {
            Json::Value pendingEvents{Json::arrayValue};
            for (auto const& event : events_[ChainType::locking])
            {
                std::visit(
                    [&](auto const& e) { pendingEvents.append(e.toJson()); },
                    event);
            }
            for (auto const& event : events_[ChainType::issuing])
            {
                std::visit(
                    [&](auto const& e) { pendingEvents.append(e.toJson()); },
                    event);
            }
            ret["pending_events"] = pendingEvents;
        }
    }

    for (ChainType ct : {ChainType::locking, ChainType::issuing})
    {
        Json::Value side{Json::objectValue};
        side["initiating"] = initSync_[ct].syncing_ ? "True" : "False";
        side["ledger_index"] = chains_[ct].listener_->getCurrentLedger();
        side["fee"] = chains_[ct].listener_->getCurrentFee();

        int commitCount = 0;
        int createCount = 0;
        Json::Value commitAttests{Json::arrayValue};
        Json::Value createAttests{Json::arrayValue};
        auto getAttests = [&](auto const& submissions) {
            commitCount = 0;
            createCount = 0;
            commitAttests.clear();
            createAttests.clear();
            for (auto const& txn : submissions)
            {
                txn->forAttestIDs(
                    [&](std::uint64_t id) {
                        assert(
                            id <= (std::uint64_t)
                                      std::numeric_limits<std::uint32_t>::max);
                        commitAttests.append((std::uint32_t)id);
                        ++commitCount;
                    },
                    [&](std::uint64_t id) {
                        assert(
                            id <= (std::uint64_t)
                                      std::numeric_limits<std::uint32_t>::max);
                        createAttests.append((std::uint32_t)id);
                        ++createCount;
                    });
            }
        };

        {
            std::lock_guard l{txnsMutex_};
            getAttests(submitted_[ct]);
            Json::Value submitted{Json::objectValue};
            submitted["commit_attests_size"] = commitCount;
            if (commitCount)
                submitted["commit_attests"] = commitAttests;
            submitted["create_account_attests_size"] = createCount;
            if (createCount)
                submitted["create_account_attests"] = createAttests;
            side["submitted"] = submitted;

            getAttests(errored_[ct]);
            Json::Value errored{Json::objectValue};
            errored["commit_attests_size"] = commitCount;
            if (commitCount > 0)
                errored["commit_attests"] = commitAttests;
            errored["create_account_attests_size"] = createCount;
            if (createCount > 0)
                errored["create_account_attests"] = createAttests;
            side["errored"] = errored;

            getAttests(txns_[ct]);
        }
        {
            std::lock_guard l{batchMutex_};
            for (auto const& a : curClaimAtts_[ct])
            {
                commitAttests.append((int)a.claimID);
                ++commitCount;
            }
            for (auto const& a : curCreateAtts_[ct])
            {
                createAttests.append((int)a.createCount);
                ++createCount;
            }
        }
        Json::Value pending{Json::objectValue};
        pending["commit_attests_size"] = commitCount;
        if (commitCount > 0)
            pending["commit_attests"] = commitAttests;
        pending["create_account_attests_size"] = createCount;
        if (createCount > 0)
            pending["create_account_attests"] = createAttests;
        side["pending"] = pending;

        ret[to_string(ct)] = side;
    }

    return ret;
}

void
Federator::pullAndAttestTx(
    ripple::STXChainBridge const& bridge,
    ChainType ct,
    ripple::uint256 const& txHash,
    Json::Value& result)
{
    // TODO multi bridge
    if (isSyncing())
    {
        result["error"] = "syncing";
        return;
    }

    auto callback = [this, srcChain = ct](Json::Value const& v) {
        chains_[srcChain].listener_->processTx(v);
    };

    Json::Value request;
    request[ripple::jss::transaction] = to_string(txHash);
    chains_[ct].listener_->send("tx", request, callback);
}

std::size_t
Federator::maxAttests() const
{
#ifdef USE_BATCH_ATTESTATION
    return useBatch_ ? ripple::AttestationBatch::maxAttestations : 1;
#else
    return 1;
#endif
}

void
Federator::checkSigningKey(
    ChainType const ct,
    bool const masterDisabled,
    std::optional<ripple::AccountID> const& regularAcc)
{
    auto const signingAcc = ripple::calcAccountID(signingPK_);

    if ((signingAcc == signingAccount_) && masterDisabled)
    {
        JLOGV(
            j_.fatal(),
            "Masterkey disabled for signing account",
            jv("chainType", to_string(ct)),
            jv("config acc", ripple::toBase58(*signingAccount_)),
            jv("config pk acc", ripple::toBase58(signingAcc)),
            jv("info regular acc",
               regularAcc ? ripple::toBase58(*regularAcc) : std::string()));
        throw std::runtime_error("Masterkey disabled for signing account");
    }

    if ((signingAcc != signingAccount_) && (regularAcc != signingAcc))
    {
        JLOGV(
            j_.fatal(),
            "Invalid signing account regular key used",
            jv("chainType", to_string(ct)),
            jv("config acc", ripple::toBase58(*signingAccount_)),
            jv("config pk acc", ripple::toBase58(signingAcc)),
            jv("info regular acc",
               regularAcc ? ripple::toBase58(*regularAcc) : std::string()));

        throw std::runtime_error("Invalid signing account key used");
    }

    if (signingAcc == signingAccount_)
    {
        JLOGV(
            j_.trace(),
            "Signing account master key used",
            jv("acc", ripple::toBase58(*signingAccount_)));
    }
    else
    {
        JLOGV(
            j_.trace(),
            "Signing account regular key used",
            jv("master acc", ripple::toBase58(*signingAccount_)),
            jv("regular acc",
               regularAcc ? ripple::toBase58(*regularAcc) : std::string()));
    }
}

void
Federator::setNetworkID(std::uint32_t networkID, ChainType ct)
{
    if (networkID_[ct] != networkID)
    {
        networkID_[ct] = networkID;

        // Fix NetworkID for tx from DB
        std::lock_guard tl{txnsMutex_};
        auto& subs(submitted_[ct]);
        for (auto& s : subs)
            s->networkID_ = networkID;
    }
}

ripple::AccountID
Federator::getSigningAccount() const
{
    return signingAccount_ ? *signingAccount_
                           : ripple::calcAccountID(signingPK_);
}

ripple::PublicKey
Federator::getSigningPK() const
{
    return signingPK_;
}

ripple::SecretKey
Federator::getSigningSK() const
{
    return signingSK_;
}

bool
SubmissionSort::operator<(const SubmissionSort& s) const
{
    return v1 ? v1 < s.v1 : v2 < s.v2;
}

bool
SubmissionSort::operator==(const SubmissionSort& s) const
{
    return v1 ? v1 == s.v1 : v2 == s.v2;
}

Submission::Submission(
    std::uint32_t lastLedgerSeq,
    std::uint32_t accountSqn,
    std::uint32_t networkID,
    std::string_view const logName)
    : lastLedgerSeq_(lastLedgerSeq)
    , accountSqn_(accountSqn)
    , networkID_(networkID)
    , logName_(logName)
{
}

std::string const&
Submission::getLogName() const
{
    return logName_;
}

bool
Submission::operator<(const Submission& s) const
{
    return getSort() < s.getSort();
}

bool
Submission::operator==(const Submission& s) const
{
    return getSort() == s.getSort();
}

#ifdef USE_BATCH_ATTESTATION
SubmissionBatch::SubmissionBatch(
    uint32_t lastLedgerSeq,
    uint32_t accountSqn,
    ripple::STXChainAttestationBatch const& batch)
    : Submission(lastLedgerSeq, accountSqn, "batch"), batch_(batch)
{
}

std::pair<std::string, std::string>
SubmissionBatch::forAttestIDs(
    std::function<void(std::uint64_t id)> commitFunc,
    std::function<void(std::uint64_t id)> createFunc) const
{
    return xbwd::forAttestIDs(batch_, commitFunc, createFunc);
}

Json::Value
SubmissionBatch::getJson(ripple::JsonOptions const opt) const
{
    return batch_.getJson(opt);
}

std::size_t
SubmissionBatch::numAttestations() const
{
    return batch_.numAttestations();
}

ripple::STTx
SubmissionBatch::getSignedTxn(
    config::TxnSubmit const& txn,
    ripple::XRPAmount const& fee,
    beast::Journal j) const
{
    return xbwd::txn::getSignedTxn(
        txn.submittingAccount,
        batch_,
        ripple::jss::XChainAddAttestations,
        batch_.getFName().getJsonName(),
        accountSqn_,
        lastLedgerSeq_,
        fee,
        txn.keypair,
        j);
}

#endif

SubmissionClaim::SubmissionClaim(
    std::uint32_t lastLedgerSeq,
    std::uint32_t accountSqn,
    std::uint32_t networkID,
    ripple::STXChainBridge const& bridge,
    ripple::Attestations::AttestationClaim const& claim)
    : Submission(lastLedgerSeq, accountSqn, networkID, "claim")
    , bridge_(bridge)
    , claim_(claim)
{
}

std::pair<std::string, std::string>
SubmissionClaim::forAttestIDs(
    std::function<void(std::uint64_t id)> commitFunc,
    std::function<void(std::uint64_t id)> createFunc) const
{
    return xbwd::forAttestIDs(claim_, commitFunc, createFunc);
}

Json::Value
SubmissionClaim::getJson(ripple::JsonOptions const opt) const
{
    Json::Value j = claim_.toSTObject().getJson(opt);
    j[ripple::sfOtherChainSource.getJsonName()] = j[ripple::jss::Account];
    j.removeMember(ripple::jss::Account);
    j[bridge_.getFName().getJsonName()] = bridge_.getJson(opt);
    return j;
}

std::size_t
SubmissionClaim::numAttestations() const
{
    return 1;
}

ripple::STTx
SubmissionClaim::getSignedTxn(
    config::TxnSubmit const& txn,
    ripple::XRPAmount const& fee,
    beast::Journal j) const
{
    return xbwd::txn::getSignedTxn(
        txn.submittingAccount,
        *this,
        ripple::jss::XChainAddClaimAttestation,
        Json::StaticString(nullptr),
        accountSqn_,
        lastLedgerSeq_,
        networkID_,
        fee,
        txn.keypair,
        j);
}

bool
SubmissionClaim::checkID(
    std::optional<std::uint32_t> const& claim,
    std::optional<std::uint32_t> const&)
{
    return claim == claim_.claimID;
}

SubmissionSort
SubmissionClaim::getSort() const
{
    return {{}, claim_.claimID};
}

SubmissionCreateAccount::SubmissionCreateAccount(
    std::uint32_t lastLedgerSeq,
    std::uint32_t accountSqn,
    std::uint32_t networkID,
    ripple::STXChainBridge const& bridge,
    ripple::Attestations::AttestationCreateAccount const& create)
    : Submission(lastLedgerSeq, accountSqn, networkID, "createAccount")
    , bridge_(bridge)
    , create_(create)
{
}

std::pair<std::string, std::string>
SubmissionCreateAccount::forAttestIDs(
    std::function<void(std::uint64_t id)> commitFunc,
    std::function<void(std::uint64_t id)> createFunc) const
{
    return xbwd::forAttestIDs(create_, commitFunc, createFunc);
}

Json::Value
SubmissionCreateAccount::getJson(ripple::JsonOptions const opt) const
{
    Json::Value j = create_.toSTObject().getJson(opt);
    j[ripple::sfOtherChainSource.getJsonName()] = j[ripple::jss::Account];
    j.removeMember(ripple::jss::Account);
    j[bridge_.getFName().getJsonName()] = bridge_.getJson(opt);
    return j;
}

std::size_t
SubmissionCreateAccount::numAttestations() const
{
    return 1;
}

ripple::STTx
SubmissionCreateAccount::getSignedTxn(
    config::TxnSubmit const& txn,
    ripple::XRPAmount const& fee,
    beast::Journal j) const
{
    return xbwd::txn::getSignedTxn(
        txn.submittingAccount,
        *this,
        ripple::jss::XChainAddAccountCreateAttestation,
        Json::StaticString(nullptr),
        accountSqn_,
        lastLedgerSeq_,
        networkID_,
        fee,
        txn.keypair,
        j);
}

bool
SubmissionCreateAccount::checkID(
    std::optional<std::uint32_t> const&,
    std::optional<std::uint32_t> const& create)
{
    return create == create_.createCount;
}

SubmissionSort
SubmissionCreateAccount::getSort() const
{
    return {create_.createCount, {}};
}

Json::Value
SignerListInfo::toJson() const
{
    Json::Value result{Json::objectValue};
    result["status"] = static_cast<int>(status_);
    result["disableMaster"] = disableMaster_;
    result["regularDoorID"] = regularDoorID_.isNonZero()
        ? ripple::toBase58(regularDoorID_)
        : std::string();
    result["presentInSignerList"] = presentInSignerList_;
    result["ignoreSignerList"] = ignoreSignerList_;

    return result;
}

}  // namespace xbwd
