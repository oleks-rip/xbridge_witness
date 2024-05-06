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

#include <xbwd/client/ChainListener.h>

#include <xbwd/basics/StructuredLog.h>
#include <xbwd/client/RpcResultParse.h>
#include <xbwd/client/WebsocketClient.h>
#include <xbwd/federator/Federator.h>
#include <xbwd/federator/FederatorEvents.h>

#include <ripple/basics/Log.h>
#include <ripple/basics/RangeSet.h>
#include <ripple/basics/XRPAmount.h>
#include <ripple/basics/strHex.h>
#include <ripple/json/Output.h>
#include <ripple/json/json_writer.h>
#include <ripple/protocol/AccountID.h>
#include <ripple/protocol/ErrorCodes.h>
#include <ripple/protocol/LedgerFormats.h>
#include <ripple/protocol/SField.h>
#include <ripple/protocol/STAmount.h>
#include <ripple/protocol/TER.h>
#include <ripple/protocol/TxFlags.h>
#include <ripple/protocol/json_get_or_throw.h>
#include <ripple/protocol/jss.h>

#include <charconv>
#include <type_traits>

namespace xbwd {

class Federator;

ChainListener::ChainListener(
    ChainType chainType,
    ripple::STXChainBridge const sidechain,
    std::optional<ripple::AccountID> submitAccount,
    Federator& federator,
    std::optional<ripple::AccountID> signAccount,
    std::uint32_t txLimit,
    std::uint32_t lastLedgerProcessed,
    beast::Journal j)
    : chainType_{chainType}
    , bridge_{sidechain}
    , submitAccountStr_(
          submitAccount ? ripple::toBase58(*submitAccount) : std::string{})
    , federator_(federator)
    , signAccount_(signAccount)
    , j_{j}
    , txLimit_(txLimit)
{
    hp_.lastLedgerProcessed_ = lastLedgerProcessed;
}

void
ChainListener::init(boost::asio::io_service& ios, beast::IP::Endpoint const& ip)
{
    wsClient_ = std::make_unique<WebsocketClient>(
        [this](Json::Value const& msg) { onMessage(msg); },
        [this]() { onConnect(); },
        ios,
        ip,
        /*headers*/ std::unordered_map<std::string, std::string>{},
        j_);

    wsClient_->connect();
}

void
ChainListener::onConnect()
{
    auto const doorAccStr = ripple::toBase58(bridge_.door(chainType_));

    auto doorAccInfoCb = [this, doorAccStr](Json::Value const& msg) {
        if (!processAccountInfo(msg))
        {
            // Reconnect here cause AccountInfo not in the Cycle, so it won't be
            // requested once more.
            // All the new changes to the account will be
            // processed with transaction parsing.
            // account_tx present in the
            // Cycle, so there is no reconnect for it
            wsClient_->reconnect("Can't process Account Info");
            return;
        }
        Json::Value params;
        params[ripple::jss::streams] = Json::arrayValue;
        params[ripple::jss::streams].append("ledger");
        this->send("subscribe", params);
    };

    auto serverInfoCb =
        [this, doorAccStr, doorAccInfoCb](Json::Value const& msg) {
            processServerInfo(msg);
            Json::Value params;
            params[ripple::jss::account] = doorAccStr;
            params[ripple::jss::signer_lists] = true;
            this->send("account_info", params, doorAccInfoCb);
        };

    auto mainFlow = [this, doorAccStr, serverInfoCb]() {
        Json::Value params;
        this->send("server_info", params, serverInfoCb);
    };

    auto signAccInfoCb = [this, mainFlow](Json::Value const& msg) {
        if (!processSigningAccountInfo(msg))
        {
            wsClient_->reconnect("Can't process Signing Account Info");
            return;
        }
        mainFlow();
    };

    // Clear on re-connect
    inRequest_ = false;
    ledgerReqMax_ = 0;
    ledgerProcessedDoor_ = 0;
    ledgerProcessedSubmit_ = 0;
    prevLedgerIndex_ = 0;
    txnHistoryIndex_ = 0;
    hp_.clear();

    if (signAccount_)
    {
        Json::Value params;
        params[ripple::jss::account] = ripple::toBase58(*signAccount_);
        send("account_info", params, signAccInfoCb);
    }
    else
    {
        mainFlow();
    }
}

void
ChainListener::shutdown()
{
    wsClient_.reset();
}

std::uint32_t
ChainListener::send(std::string const& cmd, Json::Value const& params) const
{
    auto const chainName = to_string(chainType_);
    return wsClient_->send(cmd, params, chainName);
}

void
ChainListener::stopHistoricalTxns()
{
    hp_.stopHistory_ = true;
}

void
ChainListener::send(
    std::string const& cmd,
    Json::Value const& params,
    RpcCallback onResponse)
{
    auto const chainName = to_string(chainType_);
    // JLOGV(
    //     j_.trace(),
    //     "ChainListener send",
    //     jv("chainType", chainName),
    //     jv("command", cmd),
    //     jv("params", params));

    auto id = wsClient_->send(cmd, params, chainName);
    // JLOGV(j_.trace(), "ChainListener send id", jv("id", id));

    std::lock_guard lock(callbacksMtx_);
    callbacks_.emplace(id, onResponse);
}

template <class E>
void
ChainListener::pushEvent(E&& e) const
{
    static_assert(std::is_rvalue_reference_v<decltype(e)>, "");
    federator_.push(std::move(e));
}

void
ChainListener::onMessage(Json::Value const& msg)
{
    auto callbackOpt = [&]() -> std::optional<RpcCallback> {
        if (msg.isMember(ripple::jss::id) && msg[ripple::jss::id].isIntegral())
        {
            auto callbackId = msg[ripple::jss::id].asUInt();
            std::lock_guard lock(callbacksMtx_);
            auto i = callbacks_.find(callbackId);
            if (i != callbacks_.end())
            {
                auto cb = i->second;
                callbacks_.erase(i);
                return cb;
            }
        }
        return {};
    }();

    if (callbackOpt)
    {
        JLOGV(
            j_.trace(),
            "ChainListener onMessage, reply to a callback",
            jv("chainType", to_string(chainType_)),
            jv("msg", msg.toStyledString()));
        (*callbackOpt)(msg);
    }
    else
    {
        processMessage(msg);
    }
}

namespace {

bool
isDeletedClaimId(Json::Value const& meta, std::uint64_t claimID)
{
    if (!meta.isMember("AffectedNodes"))
        return false;

    for (auto const& an : meta["AffectedNodes"])
    {
        if (!an.isMember("DeletedNode"))
            continue;
        auto const& dn = an["DeletedNode"];
        if (!dn.isMember("FinalFields"))
            continue;
        auto const& ff = dn["FinalFields"];
        auto const optClaimId =
            Json::getOptional<std::uint64_t>(ff, ripple::sfXChainClaimID);

        if (optClaimId == claimID)
            return true;
    }

    return false;
}

bool
isDeletedAccCnt(Json::Value const& meta, std::uint64_t createCnt)
{
    if (!meta.isMember("AffectedNodes"))
        return false;

    for (auto const& an : meta["AffectedNodes"])
    {
        if (!an.isMember("DeletedNode"))
            continue;
        auto const& dn = an["DeletedNode"];
        if (!dn.isMember("FinalFields"))
            continue;
        auto const& ff = dn["FinalFields"];
        auto const optCreateCnt = Json::getOptional<std::uint64_t>(
            ff, ripple::sfXChainAccountCreateCount);

        if (optCreateCnt == createCnt)
            return true;
    }

    return false;
}

std::optional<event::NewLedger>
checkLedger(ChainType chainType, Json::Value const& msg)
{
    auto checkLedgerHlp =
        [&](Json::Value const& msg) -> std::optional<event::NewLedger> {
        if (msg.isMember(ripple::jss::fee_base) &&
            msg[ripple::jss::fee_base].isIntegral() &&
            msg.isMember(ripple::jss::ledger_index) &&
            msg[ripple::jss::ledger_index].isIntegral() &&
            msg.isMember(ripple::jss::reserve_base) &&
            msg.isMember(ripple::jss::reserve_inc) &&
            msg.isMember(ripple::jss::validated_ledgers))
        {
            return event::NewLedger{
                chainType,
                msg[ripple::jss::ledger_index].asUInt(),
                msg[ripple::jss::fee_base].asUInt()};
        }
        return {};
    };
    return msg.isMember(ripple::jss::result)
        ? checkLedgerHlp(msg[ripple::jss::result])
        : checkLedgerHlp(msg);
}

}  // namespace

void
ChainListener::processMessage(Json::Value const& msg)
{
    auto const chainName = to_string(chainType_);

    auto ignoreRet = [&](std::string_view reason, auto&&... v) {
        JLOGV(
            j_.trace(),
            "ignoring listener message",
            jv("chainType", chainName),
            jv("reason", reason),
            std::forward<decltype(v)>(v)...);
    };

    auto txnHistoryIndex = [&]() -> std::optional<std::int32_t> {
        // only history stream messages have the index
        if (!msg.isMember(ripple::jss::account_history_tx_index) ||
            !msg[ripple::jss::account_history_tx_index].isIntegral())
            return {};
        // values < 0 are historical txns. values >= 0 are new transactions.
        // Only the initial sync needs historical txns.
        return msg[ripple::jss::account_history_tx_index].asInt();
    }();
    bool const isHistory = txnHistoryIndex && (*txnHistoryIndex < 0);

    if (isHistory && hp_.stopHistory_)
        return ignoreRet(
            "stopped processing historical tx",
            jv(ripple::jss::account_history_tx_index.c_str(),
               *txnHistoryIndex));

    JLOGV(
        j_.trace(),
        "chain listener process message",
        jv("chainType", chainName),
        jv("msg", msg.toStyledString()));

    auto newLedgerEv = checkLedger(chainType_, msg);
    if (newLedgerEv)
    {
        ledgerIndex_ = newLedgerEv->ledgerIndex_;
        ledgerFee_ = newLedgerEv->fee_;
        pushEvent(std::move(*newLedgerEv));
        processNewLedger(newLedgerEv->ledgerIndex_);
        return;
    }

    if (!msg.isMember(ripple::jss::validated) ||
        !msg[ripple::jss::validated].asBool())
        return ignoreRet("not validated");

    if (!msg.isMember(ripple::jss::transaction))
        return ignoreRet("no tx");

    auto const& transaction = msg[ripple::jss::transaction];

    if (!msg.isMember(ripple::jss::meta))
        return ignoreRet("no meta");

    auto const& meta = msg[ripple::jss::meta];

    if (!msg.isMember(ripple::jss::engine_result_code))
        return ignoreRet("no engine result code");

    ripple::TER const txnTER = [&msg] {
        return ripple::TER::fromInt(
            msg[ripple::jss::engine_result_code].asInt());
    }();
    bool const txnSuccess = ripple::isTesSuccess(txnTER);

    auto txnTypeOpt = rpcResultParse::parseXChainTxnType(transaction);
    if (!txnTypeOpt)
        return ignoreRet("not a sidechain transaction");

    auto const txnBridge = rpcResultParse::parseBridge(transaction);
    if (txnBridge && *txnBridge != bridge_)
    {
        // Only keep transactions to or from the door account.
        // Transactions to the account are initiated by users and are are cross
        // chain transactions. Transaction from the account are initiated by
        // federators and need to be monitored for errors. There are two types
        // of transactions that originate from the door account: the second half
        // of a cross chain payment and a refund of a failed cross chain
        // payment.

        // TODO: It is a mistake to filter out based on sidechain.
        // This server should support multiple sidechains
        // Note: the federator stores a hard-coded sidechain in the
        // database, if we remove this filter we need to remove
        // sidechain from the app and listener as well
        return ignoreRet("Sidechain mismatch");
    }

    auto const txnHash = rpcResultParse::parseTxHash(transaction);
    if (!txnHash)
        return ignoreRet("no tx hash");

    auto const txnSeq = rpcResultParse::parseTxSeq(transaction);
    if (!txnSeq)
        return ignoreRet("no txnSeq");

    auto const lgrSeq = rpcResultParse::parseLedgerSeq(msg);
    if (!lgrSeq)
        return ignoreRet("no lgrSeq");

    auto const src = rpcResultParse::parseSrcAccount(transaction);
    if (!src)
        return ignoreRet("no account src");

    auto const dst = rpcResultParse::parseDstAccount(transaction, *txnTypeOpt);

    auto const ledgerBoundary = [&]() -> bool {
        if (msg.isMember(ripple::jss::account_history_boundary) &&
            msg[ripple::jss::account_history_boundary].isBool() &&
            msg[ripple::jss::account_history_boundary].asBool())
        {
            JLOGV(
                j_.trace(),
                "ledger boundary",
                jv("seq", *lgrSeq),
                jv("chainType", chainName));
            return true;
        }
        return false;
    }();

    std::optional<ripple::STAmount> deliveredAmt =
        rpcResultParse::parseDeliveredAmt(transaction, meta);

    switch (*txnTypeOpt)
    {
        case XChainTxnType::xChainClaim: {
            auto const claimID = Json::getOptional<std::uint64_t>(
                transaction, ripple::sfXChainClaimID);

            if (!claimID)
                return ignoreRet("no claimID");
            if (!dst)
                return ignoreRet("no dst in xchain claim");

            using namespace event;
            XChainTransferResult e{
                chainType_,
                *dst,
                deliveredAmt,
                *claimID,
                *lgrSeq,
                *txnHash,
                txnTER,
                txnHistoryIndex};
            pushEvent(std::move(e));
        }
        break;
        case XChainTxnType::xChainCommit: {
            auto const claimID = Json::getOptional<std::uint64_t>(
                transaction, ripple::sfXChainClaimID);

            if (!claimID)
                return ignoreRet("no claimID");
            if (!txnBridge)
                return ignoreRet("no bridge in xchain commit");

            using namespace event;
            XChainCommitDetected e{
                chainType_,
                *src,
                *txnBridge,
                deliveredAmt,
                *claimID,
                dst,
                *lgrSeq,
                *txnHash,
                txnTER,
                txnHistoryIndex,
                ledgerBoundary};
            pushEvent(std::move(e));
        }
        break;
        case XChainTxnType::xChainAccountCreateCommit: {
            auto const createCount = rpcResultParse::parseCreateCount(meta);
            if (!createCount)
                return ignoreRet("no createCount");
            if (!txnBridge)
                return ignoreRet("no bridge in xchain commit");

            auto const rewardAmt = rpcResultParse::parseRewardAmt(transaction);
            if (!rewardAmt)
                return ignoreRet("no reward amt in xchain create account");

            if (!dst)
                return ignoreRet("no dst in xchain create account");

            using namespace event;
            XChainAccountCreateCommitDetected e{
                chainType_,
                *src,
                *txnBridge,
                deliveredAmt,
                *rewardAmt,
                *createCount,
                *dst,
                *lgrSeq,
                *txnHash,
                txnTER,
                txnHistoryIndex,
                ledgerBoundary};
            pushEvent(std::move(e));
        }
        break;
        case XChainTxnType::xChainCreateBridge: {
            if (!txnBridge)
                return ignoreRet("no bridge in xChainCreateBridge");
            if (isHistory)
            {
                pushEvent(event::EndOfHistory{chainType_});
                hp_.stopHistory_ = true;
            }
        }
        break;
#ifdef USE_BATCH_ATTESTATION
        case XChainTxnType::xChainAddAttestationBatch: {
            if (rpcResultParse::fieldMatchesStr(
                    transaction,
                    ripple::jss::Account,
                    submitAccountStr_.c_str()) &&
                txnSeq)
            {
                pushEvent(
                    event::XChainAttestsResult{chainType_, *txnSeq, txnTER});
                return;
            }
            else
                return ignore_ret("not an attestation sent from this server");
        }
        break;
#endif
        case XChainTxnType::xChainAddAccountCreateAttestation:
        case XChainTxnType::xChainAddClaimAttestation: {
            std::optional<std::uint64_t> claimID, accountCreateCount;

            bool const isOwn = rpcResultParse::fieldMatchesStr(
                transaction, ripple::jss::Account, submitAccountStr_.c_str());
            bool const isFinal = [&]() {
                if (txnTypeOpt == XChainTxnType::xChainAddClaimAttestation)
                {
                    claimID = Json::getOptional<std::uint64_t>(
                        transaction, ripple::sfXChainClaimID);
                    return claimID && isDeletedClaimId(meta, *claimID);
                }
                else if (
                    txnTypeOpt ==
                    XChainTxnType::xChainAddAccountCreateAttestation)
                {
                    accountCreateCount = Json::getOptional<std::uint64_t>(
                        transaction, ripple::sfXChainAccountCreateCount);
                    return accountCreateCount &&
                        isDeletedAccCnt(meta, *accountCreateCount);
                }
                return false;
            }();

            if ((isFinal || isOwn) && txnSeq)
            {
                JLOGV(
                    j_.trace(),
                    "Attestation processing",
                    jv("chainType", chainName),
                    jv("src", *src),
                    jv("dst",
                       !dst || !*dst ? std::string() : ripple::toBase58(*dst)),
                    jv("submitAccount", submitAccountStr_));

                auto osrc = rpcResultParse::parseOtherSrcAccount(
                    transaction, *txnTypeOpt);
                auto odst = rpcResultParse::parseOtherDstAccount(
                    transaction, *txnTypeOpt);
                if (!osrc ||
                    ((txnTypeOpt ==
                      XChainTxnType::xChainAddAccountCreateAttestation) &&
                     !odst))
                    return ignoreRet(
                        "osrc/odst account missing",
                        jv("submitAccount", submitAccountStr_));

                if ((txnTypeOpt == XChainTxnType::xChainAddClaimAttestation) &&
                    !claimID)
                    return ignoreRet("no claimID");
                else if (
                    (txnTypeOpt ==
                     XChainTxnType::xChainAddAccountCreateAttestation) &&
                    !accountCreateCount)
                    return ignoreRet("no accountCreateCount");

                pushEvent(event::XChainAttestsResult{
                    chainType_,
                    *txnSeq,
                    *txnHash,
                    txnTER,
                    isHistory,
                    isFinal,
                    *txnTypeOpt,
                    *osrc,
                    odst ? *odst : ripple::AccountID(),
                    accountCreateCount,
                    claimID});
                return;
            }
            else
                return ignoreRet(
                    "not an attestation sent from this server",
                    jv("submitAccount", submitAccountStr_));
        }
        break;
        case XChainTxnType::SignerListSet: {
            if (txnSuccess && !isHistory)
                processSignerListSet(transaction);
            else
                return ignoreRet(
                    isHistory ? "skip in history mode" : "not success");
            return;
        }
        break;
        case XChainTxnType::AccountSet: {
            if (txnSuccess && !isHistory)
                processAccountSet(transaction);
            else
                return ignoreRet(
                    isHistory ? "skip in history mode" : "not success");
            return;
        }
        break;
        case XChainTxnType::SetRegularKey: {
            if (txnSuccess && !isHistory)
                processSetRegularKey(transaction);
            else
                return ignoreRet(
                    isHistory ? "skip in history mode" : "not success");
            return;
        }
        break;
    }
}

namespace {
std::optional<std::unordered_set<ripple::AccountID>>
processSignerListSetGeneral(
    Json::Value const& msg,
    std::string_view const chainName,
    std::string_view const errTopic,
    beast::Journal j)
{
    auto warn_ret = [&](std::string_view reason) {
        JLOGV(
            j.warn(),
            errTopic,
            jv("reason", reason),
            jv("chainType", chainName),
            jv("msg", msg));
        return std::optional<std::unordered_set<ripple::AccountID>>();
    };

    if (msg.isMember("SignerQuorum"))
    {
        std::uint32_t const signerQuorum = msg["SignerQuorum"].asUInt();
        if (!signerQuorum)
            return warn_ret("'SignerQuorum' is null");
    }
    else
        return warn_ret("'SignerQuorum' missed");

    if (!msg.isMember("SignerEntries"))
        return warn_ret("'SignerEntries' missed");
    auto const& signerEntries = msg["SignerEntries"];
    if (!signerEntries.isArray())
        return warn_ret("'SignerEntries' is not an array");

    std::unordered_set<ripple::AccountID> entries;
    for (auto const& superEntry : signerEntries)
    {
        if (!superEntry.isMember("SignerEntry"))
            return warn_ret("'SignerEntry' missed");

        auto const& entry = superEntry["SignerEntry"];
        if (!entry.isMember(ripple::jss::Account))
            return warn_ret("'Account' missed");

        auto const& jAcc = entry[ripple::jss::Account];
        auto parsed = ripple::parseBase58<ripple::AccountID>(jAcc.asString());
        if (!parsed)
            return warn_ret("invalid 'Account'");

        entries.insert(parsed.value());
    }

    return {std::move(entries)};
}

}  // namespace

bool
ChainListener::processAccountInfo(Json::Value const& msg) const
{
    std::string const chainName = to_string(chainType_);
    std::string_view const errTopic = "ignoring account_info message";

    auto warn_ret = [&, this](std::string_view reason) {
        JLOGV(
            j_.warn(),
            errTopic,
            jv("reason", reason),
            jv("chainType", chainName),
            jv("msg", msg));
        return false;
    };

    try
    {
        if (msg.isMember(ripple::jss::error))
        {
            auto const& jerr = msg[ripple::jss::error];
            return jerr.isString() &&
                (jerr.asString() ==
                 ripple::RPC::get_error_info(ripple::rpcACT_NOT_FOUND).token);
        }

        if (!msg.isMember(ripple::jss::result))
            return warn_ret("'result' missed");

        auto const& jres = msg[ripple::jss::result];
        if (!jres.isMember(ripple::jss::account_data))
            return warn_ret("'account_data' missed");

        auto const& jaccData = jres[ripple::jss::account_data];
        if (!jaccData.isMember(ripple::jss::Account))
            return warn_ret("'Account' missed");

        auto const& jAcc = jaccData[ripple::jss::Account];
        auto const parsedAcc =
            ripple::parseBase58<ripple::AccountID>(jAcc.asString());
        if (!parsedAcc)
            return warn_ret("invalid 'Account'");

        // check disable master key
        {
            auto const fDisableMaster = jaccData.isMember(ripple::jss::Flags)
                ? static_cast<bool>(
                      jaccData[ripple::jss::Flags].asUInt() &
                      ripple::lsfDisableMaster)
                : false;

            pushEvent(event::XChainAccountSet{
                chainType_, *parsedAcc, fDisableMaster});
        }

        // check regular key
        {
            if (jaccData.isMember("RegularKey"))
            {
                std::string const regularKeyStr =
                    jaccData["RegularKey"].asString();
                auto opRegularDoorId =
                    ripple::parseBase58<ripple::AccountID>(regularKeyStr);

                pushEvent(event::XChainSetRegularKey{
                    chainType_,
                    *parsedAcc,
                    opRegularDoorId ? std::move(*opRegularDoorId)
                                    : ripple::AccountID()});
            }
        }

        // check signer list
        {
            if (!jaccData.isMember(ripple::jss::signer_lists))
            {
                warn_ret("'signer_lists' missed");
                return true;
            }
            auto const& jslArray = jaccData[ripple::jss::signer_lists];
            if (!jslArray.isArray() || jslArray.size() != 1)
            {
                warn_ret("'signer_lists'  isn't array of size 1");
                // empty array mean no signer_list, that's ok.
                return jslArray.isArray() && !jslArray;
            }

            auto opEntries = processSignerListSetGeneral(
                jslArray[0u], chainName, errTopic, j_);
            if (!opEntries)
                return true;

            pushEvent(event::XChainSignerListSet{
                chainType_, *parsedAcc, std::move(*opEntries)});
        }

        return true;
    }
    catch (std::exception const& e)
    {
        JLOGV(
            j_.warn(),
            errTopic,
            jv("exception", e.what()),
            jv("chainType", chainName),
            jv("msg", msg));
    }
    catch (...)
    {
        JLOGV(
            j_.warn(),
            errTopic,
            jv("exception", "unknown exception"),
            jv("chainType", chainName),
            jv("msg", msg));
    }

    return false;
}

void
ChainListener::processServerInfo(Json::Value const& msg)
{
    std::string const chainName = to_string(chainType_);
    std::string_view const errTopic = "ignoring server_info message";

    auto warn_ret = [&, this](std::string_view reason) {
        JLOGV(
            j_.warn(),
            errTopic,
            jv("reason", reason),
            jv("chainType", chainName));
    };

    try
    {
        if (!msg.isMember(ripple::jss::result))
            return warn_ret("'result' missed");

        auto const& jres = msg[ripple::jss::result];
        if (!jres.isMember(ripple::jss::info))
            return warn_ret("'info' missed");

        auto const& jinfo = jres[ripple::jss::info];

        std::uint32_t networkID = 0;
        auto checkNetworkID = [&jinfo, &networkID, this, warn_ret]() {
            if (!jinfo.isMember(ripple::jss::network_id))
                return;

            auto const& jnetID = jinfo[ripple::jss::network_id];
            if (!jnetID.isIntegral())
                return warn_ret("'network_id' invalid type");

            networkID = jnetID.asUInt();
            federator_.setNetworkID(networkID, chainType_);
        };
        checkNetworkID();

        auto checkCompleteLedgers = [&jinfo, this, warn_ret, chainName]() {
            if (!jinfo.isMember(ripple::jss::complete_ledgers))
                return warn_ret("'complete_ledgers' missed");

            auto const& jledgers = jinfo[ripple::jss::complete_ledgers];
            if (!jledgers.isString())
                return warn_ret("'complete_ledgers' invalid type");

            std::string const ledgers = jledgers.asString();

            ripple::RangeSet<std::uint32_t> rs;
            if (!ripple::from_string(rs, ledgers) || rs.empty())
                return warn_ret("'complete_ledgers' invalid value");

            auto const& interval = *rs.rbegin();
            auto const m = interval.lower();
            if (!hp_.minValidatedLedger_ || (m < hp_.minValidatedLedger_))
                hp_.minValidatedLedger_ = m;
        };
        checkCompleteLedgers();

        JLOGV(
            j_.info(),
            "server_info",
            jv("chainType", chainName),
            jv("minValidatedLedger", hp_.minValidatedLedger_),
            jv("networkID", networkID));
    }
    catch (std::exception const& e)
    {
        JLOGV(
            j_.warn(),
            errTopic,
            jv("exception", e.what()),
            jv("chainType", chainName),
            jv("msg", msg));
    }
    catch (...)
    {
        JLOGV(
            j_.warn(),
            errTopic,
            jv("exception", "unknown exception"),
            jv("chainType", chainName),
            jv("msg", msg));
    }
}

bool
ChainListener::processSigningAccountInfo(Json::Value const& msg) const
{
    std::string const chainName = to_string(chainType_);
    std::string_view const errTopic = "ignoring signing account_info message";

    auto warn_ret = [&, this](std::string_view reason) {
        JLOGV(
            j_.warn(),
            errTopic,
            jv("reason", reason),
            jv("chainType", chainName),
            jv("msg", msg));
        return false;
    };

    try
    {
        if (msg.isMember(ripple::jss::error))
        {
            auto const& jerr = msg[ripple::jss::error];
            return jerr.isString() &&
                (jerr.asString() ==
                 ripple::RPC::get_error_info(ripple::rpcACT_NOT_FOUND).token);
        }

        if (!msg.isMember(ripple::jss::result))
            return warn_ret("'result' missed");

        auto const& jres = msg[ripple::jss::result];
        if (!jres.isMember(ripple::jss::account_data))
            return warn_ret("'account_data' missed");

        auto const& jaccData = jres[ripple::jss::account_data];
        if (!jaccData.isMember(ripple::jss::Account))
            return warn_ret("'Account' missed");

        auto const& jAcc = jaccData[ripple::jss::Account];
        auto const parsedAcc =
            ripple::parseBase58<ripple::AccountID>(jAcc.asString());
        if (!parsedAcc)
            return warn_ret("invalid 'Account'");

        bool const fDisableMaster = jaccData.isMember(ripple::jss::Flags)
            ? static_cast<bool>(
                  jaccData[ripple::jss::Flags].asUInt() &
                  ripple::lsfDisableMaster)
            : false;

        std::optional<ripple::AccountID> regularAcc;
        if (jaccData.isMember("RegularKey"))
        {
            std::string const regularKeyStr = jaccData["RegularKey"].asString();
            regularAcc = ripple::parseBase58<ripple::AccountID>(regularKeyStr);
        }

        federator_.checkSigningKey(chainType_, fDisableMaster, regularAcc);

        return true;
    }
    catch (std::exception const& e)
    {
        JLOGV(
            j_.warn(),
            errTopic,
            jv("exception", e.what()),
            jv("chainType", chainName),
            jv("msg", msg));
    }
    catch (...)
    {
        JLOGV(
            j_.warn(),
            errTopic,
            jv("exception", "unknown exception"),
            jv("chainType", chainName),
            jv("msg", msg));
    }

    return false;
}

void
ChainListener::processSignerListSet(Json::Value const& msg) const
{
    std::string const chainName = to_string(chainType_);
    std::string_view const errTopic = "ignoring SignerListSet message";

    auto warn_ret = [&, this](std::string_view reason) {
        JLOGV(
            j_.warn(),
            errTopic,
            jv("reason", reason),
            jv("chainType", chainName),
            jv("msg", msg));
    };

    try
    {
        auto const lockingDoorStr =
            ripple::toBase58(bridge_.lockingChainDoor());
        auto const issuingDoorStr =
            ripple::toBase58(bridge_.issuingChainDoor());

        if (!msg.isMember(ripple::jss::Account))
            return warn_ret("'Account' missed");

        auto const txAccStr = msg[ripple::jss::Account].asString();
        if ((txAccStr != lockingDoorStr) && (txAccStr != issuingDoorStr))
            return warn_ret("unknown tx account");

        auto const parsedAcc = ripple::parseBase58<ripple::AccountID>(txAccStr);
        if (!parsedAcc)
            return warn_ret("invalid 'Account'");

        auto opEntries =
            processSignerListSetGeneral(msg, chainName, errTopic, j_);
        if (!opEntries)
            return;

        event::XChainSignerListSet evSignSet{
            .chainType_ = txAccStr == lockingDoorStr ? ChainType::locking
                                                     : ChainType::issuing,
            .masterDoorID_ = *parsedAcc,
            .signerList_ = std::move(*opEntries)};
        if (evSignSet.chainType_ != chainType_)
        {
            // This is strange but it is processed well by rippled
            // so we can proceed
            JLOGV(
                j_.warn(),
                "processing signer list message",
                jv("warning", "Door account type mismatch"),
                jv("chain_type", to_string(chainType_)),
                jv("tx_type", to_string(evSignSet.chainType_)));
        }

        pushEvent(std::move(evSignSet));
    }
    catch (std::exception const& e)
    {
        JLOGV(
            j_.warn(),
            errTopic,
            jv("exception", e.what()),
            jv("chainType", chainName),
            jv("msg", msg));
    }
    catch (...)
    {
        JLOGV(
            j_.warn(),
            errTopic,
            jv("exception", "unknown exception"),
            jv("chainType", chainName),
            jv("msg", msg));
    }
}

void
ChainListener::processAccountSet(Json::Value const& msg) const
{
    std::string const chainName = to_string(chainType_);
    std::string_view const errTopic = "ignoring AccountSet message";

    auto warn_ret = [&, this](std::string_view reason) {
        JLOGV(
            j_.warn(),
            errTopic,
            jv("reason", reason),
            jv("chainType", chainName),
            jv("msg", msg));
    };

    try
    {
        auto const lockingDoorStr =
            ripple::toBase58(bridge_.lockingChainDoor());
        auto const issuingDoorStr =
            ripple::toBase58(bridge_.issuingChainDoor());

        if (!msg.isMember(ripple::jss::Account))
            return warn_ret("'Account' missed");

        auto const txAccStr = msg[ripple::jss::Account].asString();
        if ((txAccStr != lockingDoorStr) && (txAccStr != issuingDoorStr))
            return warn_ret("unknown tx account");

        auto const parsedAcc = ripple::parseBase58<ripple::AccountID>(txAccStr);
        if (!parsedAcc)
            return warn_ret("invalid 'Account'");

        if (!msg.isMember(ripple::jss::SetFlag) &&
            !msg.isMember(ripple::jss::ClearFlag))
            return warn_ret("'XXXFlag' missed");

        bool const setFlag = msg.isMember(ripple::jss::SetFlag);
        std::uint32_t const flag = setFlag
            ? msg[ripple::jss::SetFlag].asUInt()
            : msg[ripple::jss::ClearFlag].asUInt();
        if (flag != ripple::asfDisableMaster)
            return warn_ret("not 'asfDisableMaster' flag");

        event::XChainAccountSet evAccSet{chainType_, *parsedAcc, setFlag};
        if (evAccSet.chainType_ != chainType_)
        {
            // This is strange but it is processed well by rippled
            // so we can proceed
            JLOGV(
                j_.warn(),
                "processing account set",
                jv("warning", "Door account type mismatch"),
                jv("chainType", chainName),
                jv("tx_type", to_string(evAccSet.chainType_)));
        }
        pushEvent(std::move(evAccSet));
    }
    catch (std::exception const& e)
    {
        JLOGV(
            j_.warn(),
            errTopic,
            jv("exception", e.what()),
            jv("chainType", chainName),
            jv("msg", msg));
    }
    catch (...)
    {
        JLOGV(
            j_.warn(),
            errTopic,
            jv("exception", "unknown exception"),
            jv("chainType", chainName),
            jv("msg", msg));
    }
}

void
ChainListener::processSetRegularKey(Json::Value const& msg) const
{
    std::string const chainName = to_string(chainType_);
    std::string_view const errTopic = "ignoring SetRegularKey message";

    auto warn_ret = [&, this](std::string_view reason) {
        JLOGV(
            j_.warn(),
            errTopic,
            jv("reason", reason),
            jv("chainType", chainName),
            jv("msg", msg));
    };

    try
    {
        auto const lockingDoorStr =
            ripple::toBase58(bridge_.lockingChainDoor());
        auto const issuingDoorStr =
            ripple::toBase58(bridge_.issuingChainDoor());

        if (!msg.isMember(ripple::jss::Account))
            return warn_ret("'Account' missed");

        auto const txAccStr = msg[ripple::jss::Account].asString();
        if ((txAccStr != lockingDoorStr) && (txAccStr != issuingDoorStr))
            return warn_ret("unknown tx account");

        auto const parsedAcc = ripple::parseBase58<ripple::AccountID>(txAccStr);
        if (!parsedAcc)
            return warn_ret("invalid 'Account'");

        event::XChainSetRegularKey evSetKey{
            .chainType_ = txAccStr == lockingDoorStr ? ChainType::locking
                                                     : ChainType::issuing,
            .masterDoorID_ = *parsedAcc};

        if (evSetKey.chainType_ != chainType_)
        {
            // This is strange but it is processed well by rippled
            // so we can proceed
            JLOGV(
                j_.warn(),
                "processing account set",
                jv("warning", "Door account type mismatch"),
                jv("chainType", chainName),
                jv("tx_type", to_string(evSetKey.chainType_)));
        }

        std::string const regularKeyStr = msg.isMember("RegularKey")
            ? msg["RegularKey"].asString()
            : std::string();
        if (!regularKeyStr.empty())
        {
            auto opRegularDoorId =
                ripple::parseBase58<ripple::AccountID>(regularKeyStr);
            if (!opRegularDoorId)
                return warn_ret("invalid 'RegularKey'");
            evSetKey.regularDoorID_ = std::move(*opRegularDoorId);
        }

        pushEvent(std::move(evSetKey));
    }
    catch (std::exception const& e)
    {
        JLOGV(
            j_.warn(),
            errTopic,
            jv("exception", e.what()),
            jv("chainType", chainName),
            jv("msg", msg));
    }
    catch (...)
    {
        JLOGV(
            j_.warn(),
            errTopic,
            jv("exception", "unknown exception"),
            jv("chainType", chainName),
            jv("msg", msg));
    }
}

void
ChainListener::processTx(Json::Value const& v) const
{
    std::string const chainName = to_string(chainType_);
    std::string_view const errTopic = "ignoring tx RPC response";

    auto warn_ret = [&, this](
                        std::string_view reason,
                        beast::severities::Severity severity =
                            beast::severities::kWarning) {
        JLOGV(
            j_.stream(severity),
            errTopic,
            jv("reason", reason),
            jv("chainType", chainName),
            jv("msg", v));
    };

    try
    {
        if (!v.isMember(ripple::jss::result))
            return warn_ret("missing result field");

        auto const& msg = v[ripple::jss::result];

        if (!msg.isMember(ripple::jss::validated) ||
            !msg[ripple::jss::validated].asBool())
            return warn_ret("not validated");

        if (!msg.isMember(ripple::jss::meta))
            return warn_ret("missing meta field");

        auto const& meta = msg[ripple::jss::meta];

        if (!(meta.isMember("TransactionResult") &&
              meta["TransactionResult"].isString() &&
              meta["TransactionResult"].asString() == "tesSUCCESS"))
            return warn_ret("missing or bad TransactionResult");

        auto txnTypeOpt = rpcResultParse::parseXChainTxnType(msg);
        if (!txnTypeOpt)
            return warn_ret("missing or bad tx type");

        auto const txnHash = rpcResultParse::parseTxHash(msg);
        if (!txnHash)
            return warn_ret("missing or bad tx hash");

        auto const txnBridge = rpcResultParse::parseBridge(msg);
        if (txnBridge != bridge_)
            return warn_ret("missing or bad bridge", beast::severities::kTrace);

        auto const txnSeq = rpcResultParse::parseTxSeq(msg);
        if (!txnSeq)
            return warn_ret("missing or bad tx sequence");

        auto const lgrSeq = rpcResultParse::parseLedgerSeq(msg);
        if (!lgrSeq)
            return warn_ret("missing or bad ledger sequence");

        auto const src = rpcResultParse::parseSrcAccount(msg);
        if (!src)
            return warn_ret("missing or bad source account");

        auto const dst = rpcResultParse::parseDstAccount(msg, *txnTypeOpt);

        std::optional<ripple::STAmount> deliveredAmt =
            rpcResultParse::parseDeliveredAmt(msg, meta);

        switch (*txnTypeOpt)
        {
            case XChainTxnType::xChainCommit: {
                auto const claimID = Json::getOptional<std::uint64_t>(
                    msg, ripple::sfXChainClaimID);
                if (!claimID)
                    return warn_ret("no claimID");

                using namespace event;
                XChainCommitDetected e{
                    chainType_,
                    *src,
                    *txnBridge,
                    deliveredAmt,
                    *claimID,
                    dst,
                    *lgrSeq,
                    *txnHash,
                    ripple::tesSUCCESS,
                    {},
                    false};
                pushEvent(std::move(e));
            }
            break;
            case XChainTxnType::xChainAccountCreateCommit: {
                auto const createCount = rpcResultParse::parseCreateCount(meta);
                if (!createCount)
                    return warn_ret("missing or bad createCount");

                auto const rewardAmt = rpcResultParse::parseRewardAmt(msg);
                if (!rewardAmt)
                    return warn_ret("missing or bad rewardAmount");

                if (!dst)
                    return warn_ret("missing or bad destination account");

                using namespace event;
                XChainAccountCreateCommitDetected e{
                    chainType_,
                    *src,
                    *txnBridge,
                    deliveredAmt,
                    *rewardAmt,
                    *createCount,
                    *dst,
                    *lgrSeq,
                    *txnHash,
                    ripple::tesSUCCESS,
                    {},
                    false};
                pushEvent(std::move(e));
            }
            break;
            default:
                return warn_ret("wrong transaction type");
        }
    }
    catch (std::exception const& e)
    {
        JLOGV(
            j_.warn(),
            errTopic,
            jv("exception", e.what()),
            jv("chainType", chainName),
            jv("msg", v));
    }
    catch (...)
    {
        JLOGV(
            j_.warn(),
            errTopic,
            jv("exception", "unknown exception"),
            jv("chainType", chainName),
            jv("msg", v));
    }
}

std::uint32_t
ChainListener::getDoorProcessedLedger() const
{
    return ledgerProcessedDoor_;
}

std::uint32_t
ChainListener::getSubmitProcessedLedger() const
{
    return ledgerProcessedSubmit_;
}

std::uint32_t
ChainListener::getCurrentLedger() const
{
    return ledgerIndex_;
}

std::uint32_t
ChainListener::getCurrentFee() const
{
    return ledgerFee_;
}

std::uint32_t
ChainListener::getHistoryProcessedLedger() const
{
    return hp_.ledgerProcessed_;
}

void
ChainListener::processAccountTx(Json::Value const& msg)
{
    bool const requestContinue = processAccountTxHlp(msg);

    if (hp_.state_ == HistoryProcessor::WAIT_CB)
        hp_.state_ = HistoryProcessor::RETR_HISTORY;

    if (hp_.state_ != HistoryProcessor::FINISHED)
    {
        if (hp_.stopHistory_)
        {
            inRequest_ = false;
            txnHistoryIndex_ = 0;
            hp_.state_ = HistoryProcessor::FINISHED;
            JLOGV(
                j_.info(),
                "History mode off",
                jv("chainType", to_string(chainType_)));
        }
        else if (!requestContinue)
            requestLedgers();
    }
    else if (!requestContinue)
        inRequest_ = false;
}

bool
ChainListener::processAccountTxHlp(Json::Value const& msg)
{
    static std::string const errTopic = "ignoring account_tx response";

    auto const chainName = to_string(chainType_);

    auto warnMsg = [&, this](std::string_view reason, auto&&... ts) {
        JLOGV(
            j_.warn(),
            errTopic,
            jv("reason", reason),
            jv("chainType", chainName),
            jv("msg", msg),
            std::forward<decltype(ts)>(ts)...);
    };

    auto warnCont = [&, this](std::string_view reason, auto&&... ts) {
        JLOGV(
            j_.warn(),
            errTopic,
            jv("reason", reason),
            jv("chainType", chainName),
            std::forward<decltype(ts)>(ts)...);
    };

    if (ripple::RPC::contains_error(msg))
    {
        // When rippled synchronized this can happen. It is not critical.
        // Request this ledger once more time later
        warnMsg("error in msg");
        return false;
    }

    if (!msg.isMember(ripple::jss::result))
    {
        warnMsg("no result");
        throw std::runtime_error("processAccountTx no result");
    }

    auto const& result = msg[ripple::jss::result];

    if (!result.isMember(ripple::jss::ledger_index_max) ||
        !result[ripple::jss::ledger_index_max].isIntegral() ||
        !result.isMember(ripple::jss::ledger_index_min) ||
        !result[ripple::jss::ledger_index_min].isIntegral())
    {
        warnMsg("no ledger range");
        throw std::runtime_error("processAccountTx no ledger range");
    }

    // these should left the same during full account_tx + marker serie
    std::uint32_t const ledgerMax =
        result[ripple::jss::ledger_index_max].asUInt();
    std::uint32_t const ledgerMin =
        result[ripple::jss::ledger_index_min].asUInt();

    if ((hp_.state_ != HistoryProcessor::FINISHED) && ledgerMin &&
        (ledgerMin <= hp_.toRequestLedger_))
        hp_.toRequestLedger_ = ledgerMin - 1;

    if (hp_.stopHistory_ && (ledgerMax <= hp_.startupLedger_))
    {
        warnCont(
            "stopped processing historical request",
            jv("startupLedger", hp_.startupLedger_),
            jv("ledgerMax", ledgerMax));
        return false;
    }

    if (!result.isMember(ripple::jss::account) ||
        !result[ripple::jss::account].isString())
    {
        warnMsg("no account");
        throw std::runtime_error("processAccountTx no account");
    }
    auto const account = result[ripple::jss::account].asString();

    if (!result.isMember(ripple::jss::transactions) ||
        !result[ripple::jss::transactions].isArray())
    {
        warnMsg("no transactions");
        throw std::runtime_error("processAccountTx no transactions");
    }

    auto const& transactions = result[ripple::jss::transactions];
    bool const isMarker = result.isMember("marker");
    std::uint32_t cnt = 0;
    for (auto it = transactions.begin(); it != transactions.end(); ++it, ++cnt)
    {
        if ((hp_.state_ != HistoryProcessor::FINISHED) && hp_.stopHistory_)
            break;

        if (hp_.accoutTxProcessed_ &&
            (hp_.state_ == HistoryProcessor::CHECK_LEDGERS))
        {
            // Skip records from previous request if ledgers were not present at
            // that moment
            if (cnt < hp_.accoutTxProcessed_)
                continue;
            hp_.accoutTxProcessed_ = 0;
            warnCont("Skipping tx", jv("cnt", cnt));
        }

        auto const& entry(*it);

        auto next = it;
        ++next;
        bool const isLast = next == transactions.end();

        if (!entry.isMember(ripple::jss::meta))
        {
            warnCont("no meta", jv("entry", entry));
            throw std::runtime_error("processAccountTx no meta");
        }
        auto const& meta = entry[ripple::jss::meta];

        if (!entry.isMember(ripple::jss::tx))
        {
            warnCont("no tx", jv("entry", entry));
            throw std::runtime_error("processAccountTx no tx");
        }
        auto const& tx = entry[ripple::jss::tx];

        Json::Value history = Json::objectValue;

        if (!tx.isMember(ripple::jss::ledger_index) ||
            !tx[ripple::jss::ledger_index].isIntegral())
        {
            warnCont("no ledger_index", jv("entry", entry));
            throw std::runtime_error("processAccountTx no ledger_index");
        }
        std::uint32_t const ledgerIdx = tx[ripple::jss::ledger_index].asUInt();
        bool const isHistorical = hp_.startupLedger_ >= ledgerIdx;

        if (isHistorical && hp_.stopHistory_)
        {
            warnCont(
                "stopped processing historical tx",
                jv("startupLedger", hp_.startupLedger_),
                jv("txLedger", ledgerIdx));
            continue;
        }

        // if (!isMarker && isLast && isHistorical)
        //     history[ripple::jss::account_history_tx_first] = true;

        if ((hp_.state_ != HistoryProcessor::FINISHED) &&
            (prevLedgerIndex_ != ledgerIdx))
            hp_.ledgerProcessed_ = prevLedgerIndex_;

        bool const lgrBdr = (hp_.state_ != HistoryProcessor::FINISHED)
            ? prevLedgerIndex_ != ledgerIdx
            : isLast;
        if (lgrBdr)
        {
            prevLedgerIndex_ = ledgerIdx;
            history[ripple::jss::account_history_boundary] = true;
        }
        history[ripple::jss::account_history_tx_index] =
            isHistorical ? --txnHistoryIndex_ : txnHistoryIndex_++;
        std::string const tr = meta["TransactionResult"].asString();
        history[ripple::jss::engine_result] = tr;
        auto const tc = ripple::transCode(tr);
        if (!tc)
        {
            warnCont("no TransactionResult", jv("entry", entry));
            throw std::runtime_error("processAccountTx no TransactionResult");
        }
        history[ripple::jss::engine_result_code] = *tc;
        history[ripple::jss::ledger_index] = ledgerIdx;
        history[ripple::jss::meta] = meta;
        history[ripple::jss::transaction] = tx;
        history[ripple::jss::validated] =
            entry[ripple::jss::validated].asBool();
        history[ripple::jss::type] = ripple::jss::transaction;

        processMessage(history);
    }

    if (hp_.accoutTxProcessed_ && (cnt >= hp_.accoutTxProcessed_) &&
        (hp_.state_ == HistoryProcessor::CHECK_LEDGERS))
        warnCont("Skipped tx", jv("cnt", cnt));

    if (!isMarker && !hp_.stopHistory_)
        hp_.accoutTxProcessed_ = cnt;

    // Everything is ok, update last processed ledger for new transactions
    if (!isMarker && (hp_.state_ == HistoryProcessor::FINISHED))
    {
        auto const doorAccStr = ripple::toBase58(bridge_.door(chainType_));
        if (account == doorAccStr)
            ledgerProcessedDoor_ = ledgerMax;
        else if (account == submitAccountStr_)
            ledgerProcessedSubmit_ = ledgerMax;
    }

    // if history finished, check if we reach lastProcessedLedger
    if (!isMarker && (hp_.state_ != HistoryProcessor::FINISHED) &&
        !hp_.stopHistory_)
    {
        if (ledgerMin && (ledgerMin <= hp_.lastLedgerProcessed_))
        {
            JLOGV(
                j_.info(),
                "Reach last processed ledger",
                jv("chainType", chainName),
                jv("finish_ledger", hp_.toRequestLedger_ + 1),
                jv("ledgerMin", ledgerMin),
                jv("lastLedgerProcessed", hp_.lastLedgerProcessed_));
            hp_.stopHistory_ = true;
            pushEvent(event::EndOfHistory{chainType_});
        }
    }

    if (isMarker &&
        ((hp_.state_ == HistoryProcessor::FINISHED) || !hp_.stopHistory_))
    {
        std::string const account = result[ripple::jss::account].asString();
        accountTx(account, ledgerMin, ledgerMax, result[ripple::jss::marker]);
        return true;
    }

    return false;
}

void
ChainListener::accountTx(
    std::string const& account,
    std::uint32_t ledger_min,
    std::uint32_t ledger_max,
    Json::Value const& marker)
{
    inRequest_ = true;
    if (hp_.state_ != HistoryProcessor::FINISHED)
        hp_.marker_ = marker;

    Json::Value txParams;
    txParams[ripple::jss::account] = account;

    if (ledger_min)
        txParams[ripple::jss::ledger_index_min] = ledger_min;
    else
        txParams[ripple::jss::ledger_index_min] = -1;

    if (ledger_max)
        txParams[ripple::jss::ledger_index_max] = ledger_max;
    else
        txParams[ripple::jss::ledger_index_max] = -1;

    txParams[ripple::jss::binary] = false;
    txParams[ripple::jss::limit] = txLimit_;
    txParams[ripple::jss::forward] = hp_.state_ == HistoryProcessor::FINISHED;
    if (!marker.isNull())
        txParams[ripple::jss::marker] = marker;
    send("account_tx", txParams, [this](Json::Value const& msg) {
        processAccountTx(msg);
    });
}

void
ChainListener::requestLedgers()
{
    if ((hp_.state_ == HistoryProcessor::RETR_HISTORY) ||
        (hp_.state_ == HistoryProcessor::CHECK_LEDGERS))
    {
        hp_.state_ = HistoryProcessor::RETR_LEDGERS;
        sendLedgerReq(hp_.requestLedgerBatch_);
    }
}

void
ChainListener::sendLedgerReq(std::uint32_t cnt)
{
    auto const chainName = to_string(chainType_);

    if (!cnt || (hp_.toRequestLedger_ < minUserLedger_))
    {
        hp_.state_ = HistoryProcessor::CHECK_LEDGERS;
        JLOGV(
            j_.info(),
            "Finished requesting ledgers",
            jv("chainType", chainName),
            jv("finish_ledger", hp_.toRequestLedger_ + 1));
        return;
    }

    if (cnt == hp_.requestLedgerBatch_)
    {
        JLOGV(
            j_.info(),
            "History not completed, start requesting more ledgers",
            jv("chainType", chainName),
            jv("start_ledger", hp_.toRequestLedger_));
    }

    auto ledgerReqCb = [this, cnt](Json::Value const&) {
        sendLedgerReq(cnt - 1);
    };
    Json::Value params;
    params[ripple::jss::ledger_index] = hp_.toRequestLedger_--;
    send("ledger_request", params, ledgerReqCb);
}

void
ChainListener::initStartupLedger(std::uint32_t ledger)
{
    if (hp_.startupLedger_ < ledger)
    {
        if (hp_.lastLedgerProcessed_ > ledger)
        {
            JLOGV(
                j_.error(),
                "New ledger less then processed ledger",
                jv("chainType", to_string(chainType_)),
                jv("newLedger", ledger),
                jv("lastLedgerProcessed", hp_.lastLedgerProcessed_));
            throw std::runtime_error("New ledger less then processed ledger");
        }

        hp_.startupLedger_ = ledger;
        hp_.toRequestLedger_ = ledger - 1;
        JLOGV(
            j_.info(),
            "Init startup ledger",
            jv("chainType", to_string(chainType_)),
            jv("startup_ledger", hp_.startupLedger_));

        if (!ledger)
        {
            JLOGV(
                j_.error(),
                "New ledger invalid idx",
                jv("chainType", to_string(chainType_)),
                jv("ledgerIdx", ledger));
            throw std::runtime_error("New ledger invalid idx");
        }
    }
}

void
ChainListener::processNewLedger(std::uint32_t ledgerIdx)
{
    auto const doorAccStr = ripple::toBase58(bridge_.door(chainType_));

    if (!hp_.startupLedger_)
        initStartupLedger(ledgerIdx);

    if (hp_.state_ == HistoryProcessor::FINISHED)
    {
        if (inRequest_)
            return;

        if (ledgerIdx > ledgerReqMax_)
        {
            auto ledgerReqMinDoor = ledgerProcessedDoor_
                ? ledgerProcessedDoor_ + 1
                : hp_.startupLedger_ + 1;
            ledgerReqMinDoor = std::min(ledgerReqMinDoor, ledgerIdx);
            ledgerReqMax_ = ledgerIdx;
            accountTx(doorAccStr, ledgerReqMinDoor, ledgerReqMax_);
            if (!submitAccountStr_.empty())
            {
                auto ledgerReqMinSub = ledgerProcessedSubmit_
                    ? ledgerProcessedSubmit_ + 1
                    : hp_.startupLedger_ + 1;
                ledgerReqMinSub = std::min(ledgerReqMinSub, ledgerIdx);
                accountTx(submitAccountStr_, ledgerReqMinSub, ledgerReqMax_);
            }
        }

        return;
    }

    // History mode
    switch (hp_.state_)
    {
        case HistoryProcessor::CHECK_BRIDGE: {
            auto checkBridgeCb = [this, doorAccStr](Json::Value const& msg) {
                if (!processBridgeReq(msg))
                {
                    JLOGV(
                        j_.info(),
                        "History mode off, no bridge",
                        jv("chainType", to_string(chainType_)));
                    hp_.state_ = HistoryProcessor::FINISHED;
                    hp_.stopHistory_ = true;
                    pushEvent(event::EndOfHistory{chainType_});
                    return;
                }
                // request for accountTx on next "new ledger" event (in case
                // bridge just created in the latest ledger)
                hp_.state_ = HistoryProcessor::RETR_HISTORY;
            };

            hp_.state_ = HistoryProcessor::WAIT_CB;
            Json::Value params;
            params[ripple::jss::bridge_account] = doorAccStr;
            params[ripple::jss::bridge] =
                bridge_.getJson(ripple::JsonOptions::none);
            send("ledger_entry", params, checkBridgeCb);

            break;
        }
        case HistoryProcessor::CHECK_LEDGERS: {
            JLOGV(
                j_.warn(),
                "Witness NOT initialized, waiting for ledgers",
                jv("chainType", to_string(chainType_)));

            auto serverInfoCb = [this, doorAccStr](Json::Value const& msg) {
                auto const currentMinLedger = hp_.minValidatedLedger_;
                processServerInfo(msg);
                // check if ledgers were retrieved
                if (!currentMinLedger ||
                    (hp_.minValidatedLedger_ < currentMinLedger) ||
                    (hp_.minValidatedLedger_ <= minUserLedger_))
                {
                    accountTx(
                        doorAccStr,
                        hp_.lastLedgerProcessed_,
                        hp_.startupLedger_,
                        hp_.marker_);
                }
            };
            Json::Value params;
            send("server_info", params, serverInfoCb);

            break;
        }
        case HistoryProcessor::RETR_HISTORY: {
            // runs only once per initialization
            hp_.state_ = HistoryProcessor::WAIT_CB;
            // starts history from the latest ledger
            initStartupLedger(ledgerIdx);
            accountTx(doorAccStr, hp_.lastLedgerProcessed_, hp_.startupLedger_);
            break;
        }
        case HistoryProcessor::RETR_LEDGERS:
            [[fallthrough]];
        case HistoryProcessor::WAIT_CB:
            [[fallthrough]];
        default:
            break;
    }
}

bool
ChainListener::processBridgeReq(Json::Value const& msg) const
{
    if (msg.isMember(ripple::jss::error) && !msg[ripple::jss::error].isNull())
        return false;

    if (!msg.isMember(ripple::jss::status) ||
        !msg[ripple::jss::status].isString() ||
        (msg[ripple::jss::status].asString() != "success"))
        return false;

    if (!msg.isMember(ripple::jss::result))
        return false;
    auto const& res = msg[ripple::jss::result];

    if (!res.isMember(ripple::jss::ledger_current_index) ||
        !res[ripple::jss::ledger_current_index].isIntegral())
        return false;

    if (!res.isMember(ripple::jss::node))
        return false;
    auto const& node = res[ripple::jss::node];
    auto const bridge = rpc::optFromJson<ripple::STXChainBridge>(
        node, ripple::sfXChainBridge.getJsonName());
    return bridge == bridge_;
}

Json::Value
ChainListener::getInfo() const
{
    // TODO
    Json::Value ret{Json::objectValue};
    return ret;
}

void
HistoryProcessor::clear()
{
    state_ = CHECK_BRIDGE;
    stopHistory_ = false;
    marker_.clear();
    accoutTxProcessed_ = 0;
    startupLedger_ = 0;
    toRequestLedger_ = 0;
    minValidatedLedger_ = 0;
}

}  // namespace xbwd
