package main

type StateVerifyer struct {
	verifyDB         StateDatabase
	baseDB           StateDatabase
	perfConfig       PerfConfig
	taskChan         chan VerifyTask
	blockHeight      uint64
	successBatch     uint64
	failBatch        uint64
	keyCache         *InsertedKeySet
	accountKeyCache  *InsertedKeySet
	storageOwnerList []string
	storageCache     map[string][]string
}

func NewStateVerifyer(
	baseDB StateDatabase,
	verifyDB StateDatabase,
	config PerfConfig,
	taskBufferSize int, // Added a buffer size parameter for the task channel
) *StateVerifyer {
	CATrieNum := int(config.StorageTrieNum)
	verifyer := &StateVerifyer{
		verifyDB:         verifyDB,
		baseDB:           baseDB,
		perfConfig:       config,
		taskChan:         make(chan VerifyTask, taskBufferSize),
		keyCache:         NewFixedSizeSet(1000000),
		storageOwnerList: make([]string, CATrieNum),
		storageCache:     make(map[string][]string),
		accountKeyCache:  NewFixedSizeSet(AccountKeyCacheSize),
	}
	return verifyer
}

/*
func (s *StateVerifyer) Run(ctx context.Context) {
	defer close(s.taskChan)

	// init
	s.Init(int(s.perfConfig.StorageTrieNum))
	// Start task generation thread
	go s.generateRunTasks(ctx, s.perfConfig.BatchSize)

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case taskInfo := <-s.taskChan:
			s.compareHashRoot(taskInfo)
		case <-ticker.C:
			fmt.Printf(
				"[%s] verify In Progress, finish compare block %d \n",
				time.Now().Format(time.RFC3339Nano),
				s.successBatch)

		case <-ctx.Done():
			fmt.Println("Shutting down")
			return
		}
	}
}

func (s *StateVerifyer) Init(storageTrieNum int) {
	accSize := s.perfConfig.AccountsInitSize
	accBatch := s.perfConfig.AccountsBlocks
	accPerBatch := accSize / accBatch

	for i := uint64(0); i < s.perfConfig.AccountsBlocks; i++ {
		startIndex := uint64(i * accPerBatch)
		s.InitAccount(i, startIndex, accPerBatch)
	}

	log.Info("init with trie num:", storageTrieNum)
	storageBatch := uint64(s.perfConfig.StorageTrieSize)

	ownerList := genOwnerHashKey(storageTrieNum)
	for i := 0; i < len(ownerList); i++ {
		keys := genStorageTrieKey(ownerList[i], 0, storageBatch)

		vals := make([]string, 0, storageBatch)
		for j := uint64(0); j < storageBatch; j++ {
			value := generateValue(7, 16)
			vals = append(vals, string(value))
		}

		s.InitSingleStorageTrie(ownerList[i], CAKeyValue{
			Keys: keys, Vals: vals})
	}

	s.storageOwnerList = ownerList
}

func (d *StateVerifyer) InitSingleStorageTrie(
	key string,
	value CAKeyValue,
) {
	var err error
	// add new storage
	err = d.baseDB.AddStorage([]byte(key), value.Keys, value.Vals)
	if err != nil {
		fmt.Println("init base db storage err:", err.Error())
		panic(err.Error())
	}

	// add new storage
	err = d.verifyDB.AddStorage([]byte(key), value.Keys, value.Vals)
	if err != nil {
		fmt.Println("init verify storage err:", err.Error())
		panic(err.Error())
	}

	if len(d.storageCache[key]) < 100000 {
		for i := 0; i < len(value.Keys)/50; i++ {
			d.storageCache[key] = append(d.storageCache[key], value.Keys[i])
		}
	}

	// init 2 accounts to commit a block
	addresses, accounts := makeAccounts(2)
	for i := 0; i < len(addresses); i++ {
		initKey := string(crypto.Keccak256(addresses[i][:]))
		err = d.verifyDB.AddAccount(initKey, accounts[i])
		if err != nil {
			panic(err.Error())
		}
		err = d.baseDB.AddAccount(initKey, accounts[i])
		if err != nil {
			panic(err.Error())
		}
		d.accountKeyCache.Add(initKey)
	}

	var expectRoot, root common.Hash
	if expectRoot, err = d.baseDB.Commit(); err != nil {
		panic("failed to commit: " + err.Error())
	}

	if root, err = d.verifyDB.Commit(); err != nil {
		panic("failed to commit: " + err.Error())
	}

	if expectRoot != root {
		fmt.Println("error verify the root", "base db root", expectRoot, "verify db root", root)
		panic("verify fail")
	}

	fmt.Println("verify the sub tree", common.BytesToHash([]byte(key)), "success")
}

func (s *StateVerifyer) InitAccount(blockNum, startIndex, size uint64) {
	addresses, accounts := makeAccountsV2(startIndex, size)

	for i := 0; i < len(addresses); i++ {
		initKey := string(crypto.Keccak256(addresses[i][:]))
		err := s.baseDB.AddAccount(initKey, accounts[i])
		if err != nil {
			fmt.Println("init account err", err)
			panic(err.Error())
		}
		err = s.verifyDB.AddAccount(initKey, accounts[i])
		if err != nil {
			fmt.Println("init account err", err)
			panic(err.Error())
		}
	}

	var expectRoot, root common.Hash
	var err error
	if expectRoot, err = s.baseDB.Commit(); err != nil {
		panic("failed to commit: " + err.Error())
	}

	if root, err = s.verifyDB.Commit(); err != nil {
		panic("failed to commit: " + err.Error())
	}

	if expectRoot != root {
		fmt.Println("error verity the root", "base db root", expectRoot, "verify db root", root)
		panic("verify fail")
	}
	fmt.Println("verify batch of accounts success, batch number", blockNum)
}

func (s *StateVerifyer) generateRunTasks(ctx context.Context, batchSize uint64) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			taskMap := NewVerifyTask()
			random := mathrand.New(mathrand.NewSource(0))
			updateAccounts := int(batchSize) / 5 * 2
			accounts := make([][]byte, updateAccounts)
			for i := 0; i < updateAccounts; i++ {
				var (
					nonce = uint64(random.Int63())
					root  = types.EmptyRootHash
					code  = crypto.Keccak256(generateRandomBytes(20))
				)
				numBytes := random.Uint32() % 33 // [0, 32] bytes
				balanceBytes := make([]byte, numBytes)
				random.Read(balanceBytes)
				balance := new(uint256.Int).SetBytes(balanceBytes)
				data, _ := rlp.EncodeToBytes(&types.StateAccount{Nonce: nonce, Balance: balance, Root: root, CodeHash: code})
				accounts[i] = data
			}

			for i := 0; i < updateAccounts; i++ {
				randomKey, found := s.accountKeyCache.RandomItem()
				if found {
					// update the account
					taskMap.AccountTask[randomKey] = accounts[i]
				}
			}

			// small storage trie write 3/5 kv of storage
			storageUpdateNum := int(batchSize) / 5 * 3 / len(s.storageOwnerList)
			//	StorageInitSize := d.perfConfig.StorageTrieSize

			for i := 0; i < len(s.storageOwnerList); i++ {
				keys := make([]string, 0, storageUpdateNum)
				vals := make([]string, 0, storageUpdateNum)
				//owner := d.smallStorageTrie[i]
				owner := s.storageOwnerList[i]
				//fmt.Println("generate owner ", owner)
				v := s.storageCache[owner]
				//fmt.Println("small tree cache key len ", len(v))
				for j := 0; j < storageUpdateNum; j++ {
					// only cache 10000 for updating test
					randomIndex := mathrand.Intn(len(v))
					keys = append(keys, v[randomIndex])
					vals = append(vals, string(generateValue(7, 16)))
				}
				taskMap.StorageTask[owner] = CAKeyValue{Keys: keys, Vals: vals}
			}
			s.taskChan <- taskMap
		}
	}
}

func (s *StateVerifyer) compareHashRoot(taskInfo VerifyTask) {
	expectRoot, verifyRoot := s.getRootHash(taskInfo)
	s.blockHeight++
	if expectRoot != verifyRoot {
		fmt.Printf("compare hash root not same, pbss root %v, versa root %v \n",
			expectRoot, verifyRoot)
		s.failBatch++
		panic("unexpect compare hash root")
	} else {
		s.successBatch++
	}
}

func (s *StateVerifyer) getRootHash(taskInfo VerifyTask) (common.Hash, common.Hash) {
	// simulate insert and delete trie
	for owner, CAkeys := range taskInfo.StorageTask {
		err := s.UpdateStorage([]byte(owner), CAkeys.Keys, CAkeys.Vals)
		if err != nil {
			fmt.Println("update storage err", err.Error())
			panic(err.Error())
		}
	}

	for key, value := range taskInfo.AccountTask {
		err := s.UpdateAccount([]byte(key), value)
		if err != nil {
			fmt.Println("update account err", err.Error())
			panic(err.Error())
		}
	}

	baseRoot, err := s.baseDB.Commit()
	if err != nil {
		panic("state commit error" + err.Error())
	}
	verifyRoot, err := s.verifyDB.Commit()
	if err != nil {
		panic("versa commit error" + err.Error())
	}

	return baseRoot, verifyRoot
}

func (s *StateVerifyer) UpdateStorage(owner []byte, keys []string, value []string) error {
	root, err := s.verifyDB.UpdateStorage(owner, keys, value)
	if err != nil {
		return err
	}
	expecRoot, err := s.baseDB.UpdateStorage(owner, keys, value)
	if err != nil {
		return err
	}
	if expecRoot != root {
		fmt.Println("compare storage trie root not same,", "owner hash", common.BytesToHash(owner),
			"geth db root:", expecRoot,
			"versa root:", root)
	}
	return nil
}

func (s *StateVerifyer) UpdateAccount(key []byte, value []byte) error {
	err := s.verifyDB.UpdateAccount(key, value)
	if err != nil {
		return err
	}
	err = s.baseDB.UpdateAccount(key, value)
	if err != nil {
		return err
	}
	return nil
}
*/
