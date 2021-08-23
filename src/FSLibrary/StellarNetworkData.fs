// Copyright 2019 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

module StellarNetworkData

open FSharp.Data
open stellar_dotnet_sdk
open StellarCoreSet
open StellarCoreCfg
open StellarMissionContext
open Logging

// This exists to work around a buggy interaction between FSharp.Data and
// dotnet 5.0.300: __SOURCE_DIRECTORY__ is not [<Literal>] anymore, but
// JsonProvider's ResolutionFolder argument needs to be.
[<Literal>]
let cwd = __SOURCE_DIRECTORY__

type HistoryArchiveState = JsonProvider<"json-type-samples/sample-stellar-history.json", ResolutionFolder=cwd>

let PubnetLatestHistoryArchiveState =
    "http://history.stellar.org/prd/core-live/core_live_001/.well-known/stellar-history.json"

let TestnetLatestHistoryArchiveState =
    "http://history.stellar.org/prd/core-testnet/core_testnet_001/.well-known/stellar-history.json"

type PubnetNode = JsonProvider<"json-type-samples/sample-network-data.json", SampleIsList=false, ResolutionFolder=cwd>
type Tier1PublicKey = JsonProvider<"json-type-samples/sample-keys.json", SampleIsList=false, ResolutionFolder=cwd>


// Recursively check if the given quorum set only contains tier 1.
let rec checkOnlyContainsTier1 (tier1KeySet: Set<string>) (q: PubnetNode.SbQuorumSet) : bool =
    Array.forall (fun k -> Set.contains k tier1KeySet) q.Validators
    && Array.forall (checkOnlyContainsTier1Inner tier1KeySet) q.InnerQuorumSets

and checkOnlyContainsTier1Inner (tier1KeySet: Set<string>) (iq: PubnetNode.InnerQuorumSet) : bool =
    let q = new PubnetNode.SbQuorumSet(iq.JsonValue)
    checkOnlyContainsTier1 tier1KeySet q


// Recursively convert json qsets to typed QuorumSet type
let rec qsetOfNodeQset
    (pubKeysToValidators: (string array) -> Map<PeerShortName, KeyPair>)
    (q: PubnetNode.SbQuorumSet)
    : QuorumSet =
    let sz = q.Validators.Length + q.InnerQuorumSets.Length
    let pct = percentOfThreshold sz (int (q.Threshold))

    { thresholdPercent = Some pct
      validators = pubKeysToValidators q.Validators
      innerQuorumSets = Array.map (qsetOfNodeInnerQset pubKeysToValidators) q.InnerQuorumSets }

and qsetOfNodeInnerQset
    (pubKeysToValidators: (string array) -> Map<PeerShortName, KeyPair>)
    (iq: PubnetNode.InnerQuorumSet)
    : QuorumSet =
    let q = new PubnetNode.SbQuorumSet(iq.JsonValue)
    qsetOfNodeQset pubKeysToValidators q

// For testing purposes, I made these numbers much smaller
let peerCountTier1 (random: System.Random) : int = random.Next(1, 10)
let peerCountNonTier1 (random: System.Random) : int = if random.Next(2) = 0 then 8 else random.Next(1, 10)
// let peerCountTier1 (random: System.Random) : int = random.Next(25, 81)
// let peerCountNonTier1 (random: System.Random) : int = if random.Next(2) = 0 then 8 else random.Next(1, 71)

// Add `newNodes` to `original` while adding edges.
// The degree of each new node is determined by
// 1. Check if it's a tier-1 node by `tier1KeySet`.
// 2. Use peerCountTier1 or peerCountNonTier1 to decide.
//
// To preserve the degree of each existing node in `original`
// each new node's edges come from splitting an existing edge.
//
// More specifically, if we're adding a new node u,
// then we pick a random edge (a, b), remove (a, b) and add (a, u) and (u, b).
// We continue this process until u has a desired degree.
let addEdges
    (original: PubnetNode.Root array)
    (newNodes: PubnetNode.Root array)
    (tier1KeySet: Set<string>)
    (random: System.Random)
    : PubnetNode.Root array =
    let getEdgesFromNode (node: PubnetNode.Root) : (string * string) array =
        node.Peers
        |> Array.filter (fun peer -> peer < node.PublicKey) // This filter ensures that we add each edge exactly once.
        |> Array.map (fun peer -> (peer, node.PublicKey))

    let edgeArray : (string * string) array = original |> Array.map getEdgesFromNode |> Array.reduce Array.append

    // This is a silly "arraylist"
    let mutable edgeList : Map<int, string * string> =
        Array.init (Array.length edgeArray) (fun index -> (index, edgeArray.[index]))
        |> Map.ofArray

    let mutable edgeSet : Set<string * string> = edgeArray |> Set.ofArray

    printfn "edgeSet has size %d" (Set.count edgeSet)

    for newNode in newNodes do
        let u = newNode.PublicKey

        let mutable degreeRemaining =
            if Set.contains u tier1KeySet then
                peerCountTier1 random
            else
                peerCountNonTier1 random

        let maxRetryCount = 100

        for i in 1 .. maxRetryCount do
            if degreeRemaining > 0 then
                let index = random.Next(0, Set.count edgeSet)
                printfn "%d was chosen as index" index
                let edge : string * string = edgeList.[index]
                let a = fst edge
                let b = snd edge
                printfn "a = %s, b = %s, u = %s" (a.[0..5]) (b.[0..5]) (u.[0..5])
                let maybeNewEdge1 = if a < u then (a, u) else (u, a)
                let maybeNewEdge2 = if b < u then (b, u) else (u, b)
                if a = u then printfn "a = u"
                if b = u then printfn "b = u"
                if Set.contains maybeNewEdge1 edgeSet then printfn "edge 1"
                if Set.contains maybeNewEdge2 edgeSet then printfn "edge 2"

                if (a <> u)
                   && (b <> u)
                   && (not (Set.contains maybeNewEdge1 edgeSet))
                   && (not (Set.contains maybeNewEdge2 edgeSet)) then
                    degreeRemaining <- degreeRemaining - 2
                    // A bit tricky, but...
                    // 1. Replace the current edge with maybeNewEdge1
                    // 2. Append maybeNewEdge2
                    //
                    // This ensures that edgeList is exactly the list of all edges,
                    // nothing more, nothing less.
                    edgeList <- edgeList.Add(index, maybeNewEdge1)
                    edgeList <- edgeList.Add(Set.count edgeSet, maybeNewEdge2)
                    edgeSet <- edgeSet.Add(maybeNewEdge1)
                    edgeSet <- edgeSet.Add(maybeNewEdge2)
                    edgeSet <- edgeSet.Remove(edge)
                    printfn "Added an edge"

        if degreeRemaining > 0 then
            LogError "After %d attempts, we could not find an edge for %s" maxRetryCount u

    failwith "undefined 135"

let FullPubnetCoreSets (context: MissionContext) (manualclose: bool) : CoreSet list =

    if context.pubnetData.IsNone then
        failwith "pubnet simulation requires --pubnet-data=<filename.json>"

    if context.tier1Keys.IsNone then
        failwith "pubnet simulation requires --tier1-keys=<filename.json>"

    let allPubnetNodes : PubnetNode.Root array = PubnetNode.Load(context.pubnetData.Value)

    // TODO: take these counts from the context
    let tier1Cnt = 5
    let nonTier1Cnt = 10
    // A Random object with a fixed seed.
    let random = System.Random 0
    let createRandomPubKey : string = new System.String([| for i in 0 .. 10 -> "0123456789".[random.Next(10)] |])
    // TODO: create the public key randomly
    let createEmptyNode : PubnetNode.Root =
        PubnetNode.Parse(sprintf """ [{ "publicKey": "G%s" }] """ createRandomPubKey).[0]

    printfn "hello world, hopefully this gets printed before the error message"

    let newTier1Nodes = [ for i in 1 .. tier1Cnt -> createEmptyNode ] |> Array.ofList
    //            |> List.map (fun n -> { n with PubnetNode.numTotalInboundPeers = if Set.contains u tier1KeySet then peerCountTier1 random else peerCountNonTier1 random })
    let newNonTier1Nodes = [ for i in 1 .. nonTier1Cnt -> createEmptyNode ] |> Array.ofList

    let tier1KeySet : Set<string> =
        let newTier1Keys = Array.map (fun (n: PubnetNode.Root) -> n.PublicKey) newTier1Nodes in

        Tier1PublicKey.Load(context.tier1Keys.Value)
        |> Array.map (fun n -> n.PublicKey)
        |> Array.append newTier1Keys
        |> Set.ofArray


    // shuffle the nodes since the order may matter
    let newNodes =
        Array.append newTier1Nodes newNonTier1Nodes
        |> Array.sortBy (fun _ -> random.Next())

    let allPubnetNodes = addEdges allPubnetNodes newNodes tier1KeySet random

    // For each pubkey in the pubnet, we map it to an actual KeyPair (with a private
    // key) to use in the simulation. It's important to keep these straight! The keys
    // in the pubnet are _not_ used in the simulation. They are used as node identities
    // throughout the rest of this function, as strings called "pubkey", but should not
    // appear in the final CoreSets we're building.
    let mutable pubnetKeyToSimKey : Map<string, KeyPair> =
        Array.map (fun (n: PubnetNode.Root) -> (n.PublicKey, KeyPair.Random())) allPubnetNodes
        |> Map.ofArray

    // Not every pubkey used in the qsets is represented in the base set of pubnet nodes,
    // so we lazily extend the set here with new mappings as we discover new pubkeys.
    let getSimKey (pubkey: string) : KeyPair =
        match pubnetKeyToSimKey.TryFind pubkey with
        | Some (k) -> k
        | None ->
            let k = KeyPair.Random()
            pubnetKeyToSimKey <- pubnetKeyToSimKey.Add(pubkey, k)
            k

    // First we partition nodes in the network into those that have home domains and
    // those that do not. We call the former "org" nodes and the latter "misc" nodes.
    let orgNodes, miscNodes =
        Array.partition (fun (n: PubnetNode.Root) -> n.SbHomeDomain.IsSome) allPubnetNodes

    // We then trim down the set of misc nodes so that they fit within simulation
    // size limit passed. If we can't even fit the org nodes, we fail here.
    let miscNodes =
        (let numOrgNodes = Array.length orgNodes
         let numMiscNodes = Array.length miscNodes

         let _ =
             if numOrgNodes > context.networkSizeLimit then
                 failwith "simulated network size limit too small to fit org nodes"

         let takeMiscNodes = min (context.networkSizeLimit - numOrgNodes) numMiscNodes
         Array.take takeMiscNodes miscNodes)

    let allPubnetNodes = Array.append orgNodes miscNodes
    let _ = assert ((Array.length allPubnetNodes) <= context.networkSizeLimit)

    let allPubnetNodeKeys =
        Array.map (fun (n: PubnetNode.Root) -> n.PublicKey) allPubnetNodes
        |> Set.ofArray

    LogInfo "SimulatePubnet will run with %d nodes" (Array.length allPubnetNodes)

    // We then group the org nodes by their home domains. The domain names are drawn
    // from the HomeDomains of the public network but with periods replaced with dashes,
    // and lowercased, so for example keybase.io turns into keybase-io.
    let groupedOrgNodes : (HomeDomainName * PubnetNode.Root array) array =
        Array.groupBy
            (fun (n: PubnetNode.Root) ->
                let cleanOrgName = n.SbHomeDomain.Value.Replace('.', '-')
                let lowercase = cleanOrgName.ToLower()
                HomeDomainName lowercase)
            orgNodes


    // Then build a map from accountID to HomeDomainName and index-within-domain
    // for each org node.
    let orgNodeHomeDomains : Map<string, (HomeDomainName * int)> =
        Array.collect
            (fun (hdn: HomeDomainName, nodes: PubnetNode.Root array) ->
                Array.mapi (fun (i: int) (n: PubnetNode.Root) -> (n.PublicKey, (hdn, i))) nodes)
            groupedOrgNodes
        |> Map.ofArray

    // When we don't have real HomeDomainNames (eg. for misc nodes) we use a
    // node-XXXXXX name with the XXXXXX as the lowercased first 6 digits of the
    // pubkey. Lowercase because kubernetes resource objects have to have all
    // lowercase names.
    let anonymousNodeName (pubkey: string) : string =
        let simkey = (getSimKey pubkey).Address
        let extract = simkey.Substring(0, 6)
        let lowercase = extract.ToLower()
        "node-" + lowercase

    // Return either HomeDomainName name or a node-XXXXXX name derived from the pubkey.
    let homeDomainNameForKey (pubkey: string) : HomeDomainName =
        match orgNodeHomeDomains.TryFind pubkey with
        | Some (s, _) -> s
        | None -> HomeDomainName(anonymousNodeName pubkey)

    // Return either HomeDomainName-$i name or a node-XXXXXX-0 name derived from the pubkey.
    let peerShortNameForKey (pubkey: string) : PeerShortName =
        match orgNodeHomeDomains.TryFind pubkey with
        | Some (s, i) -> PeerShortName(sprintf "%s-%d" s.StringName i)
        | None -> PeerShortName((anonymousNodeName pubkey) + "-0")

    let pubKeysToValidators (pubKeys: string array) : Map<PeerShortName, KeyPair> =
        pubKeys
        |> Array.map (fun k -> (peerShortNameForKey k, getSimKey k))
        |> Map.ofArray

    let defaultQuorum : QuorumSet =
        { thresholdPercent = None
          validators =
              allPubnetNodes
              |> Array.filter (fun (n: PubnetNode.Root) -> n.SbHomeDomain = Some "www.stellar.org")
              |> Array.map (fun (n: PubnetNode.Root) -> n.PublicKey)
              |> pubKeysToValidators
          innerQuorumSets = Array.empty }

    assert (not (Map.isEmpty defaultQuorum.validators))

    let qsetOfNodeQsetOrDefault (qOption: PubnetNode.SbQuorumSet option) : QuorumSet =
        match qOption with
        | Some q ->
            if (q.Validators.Length <> 0 || q.InnerQuorumSets.Length <> 0)
               && checkOnlyContainsTier1 tier1KeySet q then
                qsetOfNodeQset pubKeysToValidators q
            else
                defaultQuorum
        | None -> defaultQuorum

    let pubnetOpts =
        { CoreSetOptions.GetDefault context.image with
              accelerateTime = false
              historyNodes = Some([])
              // We need to use a synchronized startup delay
              // for networks as large as this, otherwise it loses
              // sync before all the nodes are online.
              syncStartupDelay = Some(30)
              invariantChecks = AllInvariantsExceptBucketConsistencyChecks
              dumpDatabase = false }

    // Sorted list of known geolocations.
    // We can choose an arbitrary geolocation such that the distribution follows that of the given data
    // by uniformly randomly selecting an element from this array.
    // Since this is sorted, we will have the same assignments across different runs
    // as long as the random function is persistent.
    let geoLocations : GeoLoc array =
        allPubnetNodes
        |> Array.filter (fun (n: PubnetNode.Root) -> n.SbGeoData.IsSome)
        |> Array.map
            (fun (n: PubnetNode.Root) ->
                { lat = float n.SbGeoData.Value.Latitude
                  lon = float n.SbGeoData.Value.Longitude })
        |> Seq.ofArray
        |> Seq.sortBy (fun x -> x.lat, x.lon)
        |> Array.ofSeq

    // If the given node has geolocation info, use it.
    // Otherwise, pseudo-randomly select one from the list of geolocations that we are aware of.
    // Since the assignment depends on the pubnet public key,
    // this assignment is persistent across different runs.
    let getGeoLocOrDefault (n: PubnetNode.Root) : GeoLoc =
        match n.SbGeoData with
        | Some geoData -> { lat = float geoData.Latitude; lon = float geoData.Longitude }
        | None ->
            let len = Array.length geoLocations
            // A deterministic, fairly elementary hashing function that adds the ASCII code
            // of each character.
            // All we need is a deterministic, mostly randomized mapping.
            let h = n.PublicKey |> Seq.sumBy int
            geoLocations.[h % len]

    let makeCoreSetWithExplicitKeys (hdn: HomeDomainName) (options: CoreSetOptions) (keys: KeyPair array) =
        { name = CoreSetName(hdn.StringName)
          options = options
          keys = keys
          live = true }

    let preferredPeersMapForAllNodes : Map<byte [], byte [] list> =
        let getSimPubKey (k: string) = (getSimKey k).PublicKey

        allPubnetNodes
        |> Array.map
            (fun (n: PubnetNode.Root) ->
                let key = getSimPubKey n.PublicKey

                let peers =
                    n.Peers
                    |>
                    // This filtering is necessary since we intentionally remove some nodes
                    // using networkSizeLimit.
                    Array.filter (fun (k: string) -> Set.contains k allPubnetNodeKeys)
                    |> List.ofArray
                    |> List.map getSimPubKey

                (key, peers))
        |> Map.ofArray

    let keysToPreferredPeersMap (keys: KeyPair array) =
        keys
        |> Array.map (fun (k: KeyPair) -> (k.PublicKey, Map.find k.PublicKey preferredPeersMapForAllNodes))
        |> Map.ofArray

    let miscCoreSets : CoreSet array =
        Array.mapi
            (fun (_: int) (n: PubnetNode.Root) ->
                let hdn = homeDomainNameForKey n.PublicKey
                let qset = qsetOfNodeQsetOrDefault n.SbQuorumSet
                let keys = [| getSimKey n.PublicKey |]

                let coreSetOpts =
                    { pubnetOpts with
                          nodeCount = 1
                          quorumSet = ExplicitQuorum qset
                          tier1 = Some(Set.contains n.PublicKey tier1KeySet)
                          nodeLocs = Some [ getGeoLocOrDefault n ]
                          preferredPeersMap = Some(keysToPreferredPeersMap keys) }

                let shouldWaitForConsensus = manualclose
                let coreSetOpts = coreSetOpts.WithWaitForConsensus shouldWaitForConsensus
                makeCoreSetWithExplicitKeys hdn coreSetOpts keys)
            miscNodes

    let orgCoreSets : CoreSet array =
        Array.map
            (fun (hdn: HomeDomainName, nodes: PubnetNode.Root array) ->
                assert (nodes.Length <> 0)
                let qset = qsetOfNodeQsetOrDefault nodes.[0].SbQuorumSet
                let nodeList = List.ofArray nodes
                let keys = Array.map (fun (n: PubnetNode.Root) -> getSimKey n.PublicKey) nodes

                let coreSetOpts =
                    { pubnetOpts with
                          nodeCount = Array.length nodes
                          quorumSet = ExplicitQuorum qset
                          tier1 = Some(Set.contains nodes.[0].PublicKey tier1KeySet)
                          nodeLocs = Some(List.map getGeoLocOrDefault nodeList)
                          preferredPeersMap = Some(keysToPreferredPeersMap keys) }

                let shouldWaitForConsensus = manualclose
                let coreSetOpts = coreSetOpts.WithWaitForConsensus shouldWaitForConsensus
                makeCoreSetWithExplicitKeys hdn coreSetOpts keys)
            groupedOrgNodes

    Array.append miscCoreSets orgCoreSets |> List.ofArray


let GetLatestPubnetLedgerNumber _ : int =
    let has = HistoryArchiveState.Load(PubnetLatestHistoryArchiveState)
    has.CurrentLedger

let GetLatestTestnetLedgerNumber _ : int =
    let has = HistoryArchiveState.Load(TestnetLatestHistoryArchiveState)
    has.CurrentLedger


let PubnetGetCommands =
    [ PeerShortName "core_live_001", "curl -sf http://history.stellar.org/prd/core-live/core_live_001/{0} -o {1}"
      PeerShortName "core_live_002", "curl -sf http://history.stellar.org/prd/core-live/core_live_002/{0} -o {1}"
      PeerShortName "core_live_003", "curl -sf http://history.stellar.org/prd/core-live/core_live_003/{0} -o {1}" ]
    |> Map.ofList

let PubnetQuorum : QuorumSet =
    { thresholdPercent = None
      validators =
          [ PeerShortName "core_live_001",
            KeyPair.FromAccountId("GCGB2S2KGYARPVIA37HYZXVRM2YZUEXA6S33ZU5BUDC6THSB62LZSTYH")
            PeerShortName "core_live_002",
            KeyPair.FromAccountId("GCM6QMP3DLRPTAZW2UZPCPX2LF3SXWXKPMP3GKFZBDSF3QZGV2G5QSTK")
            PeerShortName "core_live_003",
            KeyPair.FromAccountId("GABMKJM6I25XI4K7U6XWMULOUQIQ27BCTMLS6BYYSOWKTBUXVRJSXHYQ") ]
          |> Map.ofList
      innerQuorumSets = [||] }

let PubnetPeers =
    [ PeerDnsName "core-live4.stellar.org"
      PeerDnsName "core-live5.stellar.org"
      PeerDnsName "core-live6.stellar.org" ]

let PubnetCoreSetOptions (image: string) =
    { CoreSetOptions.GetDefault image with
          quorumSet = ExplicitQuorum PubnetQuorum
          historyGetCommands = PubnetGetCommands
          peersDns = PubnetPeers
          accelerateTime = false
          initialization = { CoreSetInitialization.Default with waitForConsensus = true }
          dumpDatabase = false }

let TestnetGetCommands =
    [ PeerShortName "core_testnet_001",
      "curl -sf http://history.stellar.org/prd/core-testnet/core_testnet_001/{0} -o {1}"
      PeerShortName "core_testnet_002",
      "curl -sf http://history.stellar.org/prd/core-testnet/core_testnet_002/{0} -o {1}"
      PeerShortName "core_testnet_003",
      "curl -sf http://history.stellar.org/prd/core-testnet/core_testnet_003/{0} -o {1}" ]
    |> Map.ofList

let TestnetQuorum : QuorumSet =
    { thresholdPercent = None
      validators =
          [ PeerShortName "core_testnet_001",
            KeyPair.FromAccountId("GDKXE2OZMJIPOSLNA6N6F2BVCI3O777I2OOC4BV7VOYUEHYX7RTRYA7Y")
            PeerShortName "core_testnet_002",
            KeyPair.FromAccountId("GCUCJTIYXSOXKBSNFGNFWW5MUQ54HKRPGJUTQFJ5RQXZXNOLNXYDHRAP")
            PeerShortName "core_testnet_003",
            KeyPair.FromAccountId("GC2V2EFSXN6SQTWVYA5EPJPBWWIMSD2XQNKUOHGEKB535AQE2I6IXV2Z") ]
          |> Map.ofList
      innerQuorumSets = [||] }

let TestnetPeers =
    [ PeerDnsName "core-testnet1.stellar.org"
      PeerDnsName "core-testnet2.stellar.org"
      PeerDnsName "core-testnet3.stellar.org" ]

let TestnetCoreSetOptions (image: string) =
    { CoreSetOptions.GetDefault image with
          quorumSet = ExplicitQuorum TestnetQuorum
          historyGetCommands = TestnetGetCommands
          peersDns = TestnetPeers
          accelerateTime = false
          initialization = { CoreSetInitialization.Default with waitForConsensus = true }
          dumpDatabase = false }
