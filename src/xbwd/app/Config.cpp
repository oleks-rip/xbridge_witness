#include "ripple/protocol/SecretKey.h"
#include <xbwd/app/Config.h>

#include <xbwd/rpc/fromJSON.h>

#include <ripple/json/json_reader.h>
#include <ripple/protocol/AccountID.h>
#include <ripple/protocol/KeyType.h>
#include <ripple/protocol/SecretKey.h>

#include <boost/process/env.hpp>
#include <fmt/format.h>

namespace xbwd {
namespace config {

using namespace fmt::literals;

namespace {
ripple::KeyType
keyTypeFromJson(Json::Value const& jv, char const* key)
{
    using namespace std::literals;
    auto const v = jv[key];
    if (v.isNull())
        // default to secp256k1 if not specified
        return ripple::KeyType::secp256k1;

    auto const s = v.asString();
    if (s == "secp256k1"s)
        return ripple::KeyType::secp256k1;
    if (s == "ed25519"s)
        return ripple::KeyType::ed25519;

    throw std::runtime_error(
        "Unknown key type: "s + s + " while constructing a key type from json");
}
}  // namespace

std::optional<AdminConfig>
AdminConfig::make(Json::Value const& jv)
{
    AdminConfig ac;

    if (jv.isMember("Username") || jv.isMember("Password"))
    {
        // must have none or both of "Username" and "Password"
        if (!jv.isMember("Username") || !jv.isMember("Password") ||
            !jv["Username"].isString() || !jv["Password"].isString() ||
            jv["Username"].asString().empty() ||
            jv["Password"].asString().empty())
        {
            throw std::runtime_error("Admin config wrong format");
        }

        ac.pass.emplace(AdminConfig::PasswordAuth{
            jv["Username"].asString(), jv["Password"].asString()});
    }
    // may throw while parsing IPs or Subnets
    if (jv.isMember("IPs") && jv["IPs"].isArray())
    {
        for (auto const& s : jv["IPs"])
        {
            ac.addresses.emplace(boost::asio::ip::make_address(s.asString()));
        }
    }
    if (jv.isMember("Subnets") && jv["Subnets"].isArray())
    {
        for (auto const& s : jv["Subnets"])
        {
            // First, see if it's an ipv4 subnet. If not, try ipv6.
            // If that throws, then there's nothing we can do with
            // the entry.
            try
            {
                ac.netsV4.emplace_back(
                    boost::asio::ip::make_network_v4(s.asString()));
            }
            catch (boost::system::system_error const&)
            {
                ac.netsV6.emplace_back(
                    boost::asio::ip::make_network_v6(s.asString()));
            }
        }
    }

    if (ac.pass || !ac.addresses.empty() || !ac.netsV4.empty() ||
        !ac.netsV6.empty())
    {
        return std::optional<AdminConfig>{ac};
    }
    else
    {
        throw std::runtime_error("Admin config wrong format:" + jv.asString());
    }
}

TxnSubmit::TxnSubmit(Json::Value const& jv)
    : keyType{keyTypeFromJson(jv, "SigningKeyType")}
    , keypair{ripple::generateKeyPair(
          keyType,
          rpc::fromJson<ripple::Seed>(jv, "SigningKeySeed"))}
    , submittingAccount{
          rpc::fromJson<ripple::AccountID>(jv, "SubmittingAccount")}
{
    if (jv.isMember("ShouldSubmit"))
    {
        if (jv["ShouldSubmit"].isBool())
            shouldSubmit = jv["ShouldSubmit"].asBool();
        else
            throw std::runtime_error("WitnessSubmit config wrong format");
    }
}

ChainConfig::ChainConfig(Json::Value const& jv)
    : addrChainIp{rpc::fromJson<rpc::AddrEndpoint>(jv, "Endpoint")}
    , rewardAccount{rpc::fromJson<ripple::AccountID>(jv, "RewardAccount")}
{
    if (jv.isMember("TxnSubmit"))
    {
        txnSubmit.emplace(jv["TxnSubmit"]);
    }
    if (jv.isMember("LastAttestedCommitTx"))
    {
        lastAttestedCommitTx.emplace(
            rpc::fromJson<ripple::uint256>(jv, "LastAttestedCommitTx"));
    }
    if (jv.isMember("IgnoreSignerList"))
        ignoreSignerList = jv["IgnoreSignerList"].asBool();
}

Config::Config(Json::Value const& jv)
    : lockingChainConfig(jv["LockingChain"])
    , issuingChainConfig(jv["IssuingChain"])
    , addrRpcEndpoint{rpc::fromJson<rpc::AddrEndpoint>(jv, "RPCEndpoint")}
    , dataDir{rpc::fromJson<boost::filesystem::path>(jv, "DBDir")}
    , keyType{keyTypeFromJson(jv, "SigningKeyType")}
    , signingKey{ripple::generateKeyPair(
                     keyType,
                     rpc::fromJson<ripple::Seed>(jv, "SigningKeySeed"))
                     .second}
    , bridge{rpc::fromJson<ripple::STXChainBridge>(
          jv,
          ripple::sfXChainBridge.getJsonName())}
    , adminConfig{jv.isMember("Admin") ? AdminConfig::make(jv["Admin"]) : std::nullopt}
    , maxAttToSend(
          jv.isMember("MaxAttToSend") ? jv["MaxAttToSend"].asUInt() : 200)
    , txLimit(jv.isMember("TxLimit") ? jv["TxLimit"].asUInt() : 500)
    , logFile(jv.isMember("LogFile") ? jv["LogFile"].asString() : std::string())
    , logLevel(
          jv.isMember("LogLevel") ? jv["LogLevel"].asString() : std::string())
    , logSilent(jv.isMember("LogSilent") ? jv["LogSilent"].asBool() : false)
    , logSizeToRotateMb(
          jv.isMember("LogSizeToRotateMb") ? jv["LogSizeToRotateMb"].asUInt()
                                           : 0)
    , logFilesToKeep(
          jv.isMember("LogFilesToKeep") ? jv["LogFilesToKeep"].asUInt() : 0)
    , useBatch(jv.isMember("UseBatch") ? jv["UseBatch"].asBool() : false)
{
    if (jv.isMember("SigningAccount"))
        signingAccount = rpc::fromJson<ripple::AccountID>(jv, "SigningAccount");
#ifndef USE_BATCH_ATTESTATION
    if (useBatch)
        throw std::runtime_error(
            "Please compile with USE_BATCH_ATTESTATION to use Batch "
            "Attestations");
#endif
}

extern const char configTemplate[];

std::string
prepForFmt(std::string_view const sw)
{
    std::string s;
    bool bopen = true;
    for (auto const c : sw)
    {
        if (c == '{')
            s += "{{";
        else if (c == '}')
            s += "}}";
        else if (c == '`')
        {
            s += bopen ? '{' : '}';
            bopen = !bopen;
        }
        else
            s += c;
    }
    return s;
}

Json::Value
generateConfig()
{
    Json::Value jv;
    boost::process::environment const e = boost::this_process::environment();

    auto const keyType = ripple::KeyType::secp256k1;
    auto const lSubPair = ripple::randomKeyPair(keyType);
    auto const iSubPair = ripple::randomKeyPair(keyType);
    auto const witPair = ripple::randomKeyPair(keyType);

    // secure_erase
    std::string const s = fmt::format(
        fmt::runtime(prepForFmt(configTemplate)),
        "lhost"_a = e.at("LHOST").to_string(),
        "lport"_a = e.at("LPORT").to_string(),
        "lSubSeed"_a =
             ripple::toBase58(ripple::TokenType::AccountSecret, lSubPair.second),
         "lSubType"_a = to_string(keyType),
         "lSubAcc"_a = ripple::toBase58(ripple::calcAccountID(lSubPair.first)),
         "lRwdAcc"_a = e.at("LRWD_ACC").to_string(),
         "ihost"_a = e.at("IHOST").to_string(),
         "iport"_a = e.at("IPORT").to_string(),
         "iSubSeed"_a =
             ripple::toBase58(ripple::TokenType::AccountSecret, iSubPair.second),
         "iSubType"_a = to_string(keyType),
         "iSubAcc"_a = ripple::toBase58(ripple::calcAccountID(iSubPair.first)),
         "iRwdAcc"_a = e.at("IRWD_ACC").to_string(),
         "logFile"_a = e.at("LOG_FILE").to_string(),
         "dbDir"_a = e.at("DB_DIR").to_string(),
         "witSeed"_a =
             ripple::toBase58(ripple::TokenType::AccountSecret, witPair.second),
         "witType"_a = to_string(keyType),
         "lDoorAcc"_a = e.at("LDOOR_ACC").to_string(),
         "lCurr"_a = e.at("LDOOR_CUR").to_string(),
         "iDoorAcc"_a = e.at("IDOOR_ACC").to_string(),
         "iCurr"_a = e.at("IDOOR_CUR").to_string()
            );

    if (!Json::Reader().parse(s, jv))
        throw std::runtime_error("internal error while generate config");
    return jv;
}

const char configTemplate[] = R"str(
{
  "LockingChain": {
    "Endpoint": {
      "Host": "`lhost`",
      "Port": `lport`
    },
    "TxnSubmit": {
      "ShouldSubmit": true,
      "SigningKeySeed": "`lSubSeed`",
      "SigningKeyType": "`lSubType`",
      "SubmittingAccount": "`lSubAcc`"
    },
    "RewardAccount": "`lRwdAcc`"
  },
  "IssuingChain": {
    "Endpoint": {
      "Host": "`ihost`",
      "Port": `iport`
    },
    "TxnSubmit": {
      "ShouldSubmit": true,
      "SigningKeySeed": "`iSubSeed`",
      "SigningKeyType": `iSubType`",
      "SubmittingAccount": "`iSubAcc`"
    },
    "RewardAccount": "`iRwdAcc`"
  },
  "RPCEndpoint": {
    "Host": "127.0.0.3",
    "Port": 6010
  },
  "LogFile": "`logFile`",
  "LogLevel": "Trace",
  "DBDir": "`dbDir`",
  "SigningKeySeed": "`witSeed`",
  "SigningKeyType": "`witType`",
  "XChainBridge": {
    "LockingChainDoor": "`lDoorAcc`",
    "LockingChainIssue": {"currency": "`lCurr`"},
    "IssuingChainDoor": "`iDoorAcc`",
    "IssuingChainIssue": {"currency": "`iCurr`"}
  }
}
)str";

}  // namespace config
}  // namespace xbwd
