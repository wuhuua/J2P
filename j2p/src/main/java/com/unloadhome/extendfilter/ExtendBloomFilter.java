package com.unloadhome.extendfilter;

import com.unloadhome.extendhash.ExtendHash;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class ExtendBloomFilter {
    private int cellNum;

    private int offset;

    private List<ExtendBloomFilterCell> cells;

    private int keySize;

    private AtomicInteger numCount;

    private transient List<String> needToDelIds = new ArrayList<>();

    private transient List<String> needToAddIds = new ArrayList<>();

    private transient List<String> needToUpdateIds = new ArrayList<>();

    public enum IBFTypeEnum {
        Resource,
        Relation,
        ID;

        private IBFTypeEnum() {
        }
    }

    public enum DiffType {
        DELETE,
        ADD,
        UPDATE;

        private DiffType() {
        }
    }

    public ExtendBloomFilter() {
        this.numCount = new AtomicInteger(0);
    }

    private ExtendBloomFilter(int cellNum, IBFTypeEnum typeEnum, List<ExtendBloomFilterCell> cells) {
        this.cellNum = cellNum;
        this.numCount = new AtomicInteger(0);
        if (typeEnum.equals(IBFTypeEnum.Resource)) {
            this.keySize = 33;
        } else if (typeEnum.equals(IBFTypeEnum.Relation)) {
            this.keySize = 49;
        } else {
            this.keySize = 17;
        }

        if (this.cellNum % 3 == 0) {
            this.offset = this.cellNum / 3;
        } else {
            this.offset = this.cellNum / 3 + 1;
            this.cellNum = this.offset * 3;
        }

        if (cells == null || cells.size() == 0) {
            this.generateCells();
        } else {
            this.cells = cells;
        }

    }

    public static ExtendBloomFilter create(int cellNum, IBFTypeEnum typeEnum) {
        return new ExtendBloomFilter(cellNum, typeEnum, null);
    }

    public static ExtendBloomFilter create(int cellNum, IBFTypeEnum typeEnum, List<ExtendBloomFilterCell> cells) {
        return new ExtendBloomFilter(cellNum, typeEnum, cells);
    }

    public boolean isEmpty() {
        for (int i = 0; i < this.cellNum; ++i) {
            if (!((ExtendBloomFilterCell) this.cells.get(i)).isEmpty()) {
                return false;
            }
        }

        return true;
    }

    private void insert(byte[] key) {
        for (int i = 0; i < 3; ++i) {
            ((ExtendBloomFilterCell) this.cells.get(this.hashToCell(key, i))).add(key);
        }

        this.numCount.incrementAndGet();
    }

    public void insert(String id, String data) {
        byte[] key = new byte[33];
        int length = ExtendUUIDOperation.uuid2Byte(id, key, 1);
        if (length == 8) {
            key[0] = 0;
        } else {
            key[0] = 1;
        }

        ExtendHash.hashRes2Key(data.getBytes(StandardCharsets.UTF_8), key);
        this.insert(key);
    }

    public void insert(String srcId, String endId, String data) {
        byte[] key = new byte[49];
        int srcLen = ExtendUUIDOperation.uuid2Byte(srcId, key, 1);
        int endLen = ExtendUUIDOperation.uuid2Byte(endId, key, 17);
        key[0] = 0;
        if (srcLen == 16) {
            key[0] = 16;
        }

        if (endLen == 16) {
            key[0] = (byte) (key[0] | 1);
        }

        ExtendHash.hashRes2Key(data.getBytes(StandardCharsets.UTF_8), key);
        this.insert(key);
    }

    public void insert(String id) {
        byte[] key = new byte[17];
        int length = ExtendUUIDOperation.uuid2Byte(id, key, 1);
        if (length == 8) {
            key[0] = 0;
        } else {
            key[0] = 1;
        }

        this.insert(key);
    }

    public void insert(String id, byte[] dataSum) {
        byte[] key = new byte[17];
        int length = ExtendUUIDOperation.uuid2Byte(id, key, 1);
        if (length == 8) {
            key[0] = 0;
        } else {
            key[0] = 1;
        }

        this.insert(ExtendHash.mergeSum2Key(dataSum, key));
    }

    public List<String> calculatingResDifferences(ExtendBloomFilter filter) {
        if (this.cellNum != filter.cellNum) {
            //LOGGER.error("filter's cell number not equal, left:{}, right:{}", this.cellNum, filter.cellNum);
            return null;
        } else {
            this.subtract(filter);
            Set<String> resIds = new HashSet<>();
            Iterator<List<byte[]>> var3 = this.recover().values().iterator();

            while (var3.hasNext()) {
                List<byte[]> recoveredKeys = (List<byte[]>) var3.next();
                Iterator<byte[]> var5 = recoveredKeys.iterator();

                while (var5.hasNext()) {
                    byte[] keySum = (byte[]) var5.next();
                    resIds.add(this.parseResFromKeySum(keySum));
                }
            }

            return new ArrayList<>(resIds);
        }
    }

    public Map<String, List<String>> getResDifferencesByDiffType(ExtendBloomFilter filter) {
        if (this.cellNum != filter.cellNum) {
            //LOGGER.error("filter's cell number not equal, left:{}, right:{}", this.cellNum, filter.cellNum);
            return Collections.emptyMap();
        } else {
            this.subtract(filter);
            Set<String> needToDelOrUpdateList = new HashSet<>();
            Set<String> needToAddOrUpdateList = new HashSet<>();
            Map<String, List<byte[]>> recoverMap = this.recover();
            Iterator<byte[]> var5 = ((List<byte[]>) recoverMap.get("needToDelOrUpdate")).iterator();

            byte[] keySum;
            while (var5.hasNext()) {
                keySum = (byte[]) var5.next();
                needToDelOrUpdateList.add(this.parseResFromKeySum(keySum));
            }

            var5 = ((List<byte[]>) recoverMap.get("needToAddOrUpdate")).iterator();

            while (var5.hasNext()) {
                keySum = (byte[]) var5.next();
                needToAddOrUpdateList.add(this.parseResFromKeySum(keySum));
            }

            this.processDifferenceType(needToAddOrUpdateList, needToDelOrUpdateList);
            Map<String, List<String>> resultResIds = new HashMap<>();
            if (!this.isEmpty()) {
                return resultResIds;
            } else {
                resultResIds.put(DiffType.DELETE.name(), this.needToDelIds);
                resultResIds.put(DiffType.ADD.name(), this.needToAddIds);
                resultResIds.put(DiffType.UPDATE.name(), this.needToUpdateIds);
                return resultResIds;
            }
        }
    }

    public Map<String, List<Map<String, String>>> getRelDifferencesByDiffType(ExtendBloomFilter filter) {
        if (this.cellNum != filter.cellNum) {
            //LOGGER.error("filter's cell number not equal, left:{}, right:{}", this.cellNum, filter.cellNum);
            return Collections.emptyMap();
        } else {
            this.subtract(filter);
            Set<String> needToDelOrUpdateList = new HashSet<>();
            Set<String> needToAddOrUpdateList = new HashSet<>();
            Map<String, List<byte[]>> recoverMap = this.recover();
            Iterator<byte[]> var5 = ((List<byte[]>) recoverMap.get("needToDelOrUpdate")).iterator();

            byte[] keySum;
            while (var5.hasNext()) {
                keySum = (byte[]) var5.next();
                needToDelOrUpdateList.add(this.parseRelFromKeySum(keySum, (Map<String,String>) null));
            }

            var5 = ((List<byte[]>) recoverMap.get("needToAddOrUpdate")).iterator();

            while (var5.hasNext()) {
                keySum = (byte[]) var5.next();
                needToAddOrUpdateList.add(this.parseRelFromKeySum(keySum, (Map<String,String>) null));
            }

            this.processDifferenceType(needToAddOrUpdateList, needToDelOrUpdateList);
            Map<String, List<Map<String, String>>> resultRelIds = new HashMap<>();
            if (!this.isEmpty()) {
                return resultRelIds;
            } else {
                List<Map<String, String>> needToDeleteRelIds = new ArrayList<>();
                List<Map<String, String>> needToAddRelIds = new ArrayList<>();
                List<Map<String, String>> needToUpdateRelIds = new ArrayList<>();
                needToDeleteRelIds.addAll(this.getRelId(this.needToDelIds));
                needToAddRelIds.addAll(this.getRelId(this.needToAddIds));
                needToUpdateRelIds.addAll(this.getRelId(this.needToUpdateIds));
                resultRelIds.put(DiffType.DELETE.name(), needToDeleteRelIds);
                resultRelIds.put(DiffType.ADD.name(), needToAddRelIds);
                resultRelIds.put(DiffType.UPDATE.name(), needToUpdateRelIds);
                return resultRelIds;
            }
        }
    }

    public Map<String, List<String>> getIdDifferencesByDiffType(ExtendBloomFilter filter) {
        if (this.cellNum != filter.cellNum) {
            //LOGGER.error("filter's cell number not equal, left:{}, right:{}", this.cellNum, filter.cellNum);
            return Collections.emptyMap();
        } else {
            this.subtract(filter);
            List<String> delList = new ArrayList<>();
            List<String> addList = new ArrayList<>();
            Map<String, List<byte[]>> recoverMap = this.recover();
            Iterator<byte[]> var5 = ((List<byte[]>) recoverMap.get("needToDelOrUpdate")).iterator();

            byte[] keySum;
            while (var5.hasNext()) {
                keySum = (byte[]) var5.next();
                delList.add(this.parseResFromKeySum(keySum));
            }

            var5 = ((List<byte[]>) recoverMap.get("needToAddOrUpdate")).iterator();

            while (var5.hasNext()) {
                keySum = (byte[]) var5.next();
                addList.add(this.parseResFromKeySum(keySum));
            }

            Map<String, List<String>> resultResIds = new HashMap<>();
            resultResIds.put(DiffType.DELETE.name(), delList);
            resultResIds.put(DiffType.ADD.name(), addList);
            return resultResIds;
        }
    }

    private List<Map<String, String>> getRelId(List<String> ids) {
        List<Map<String, String>> result = new ArrayList<>();
        Iterator<String> var3 = ids.iterator();

        while (var3.hasNext()) {
            String id = (String) var3.next();
            Map<String, String> relId = new HashMap<>();
            String[] split2Id = id.split("_");
            relId.put(split2Id[0], split2Id[1]);
            result.add(relId);
        }

        return result;
    }

    public List<Map<String, String>> calculatingRelDifferences(ExtendBloomFilter filter) {
        if (this.cellNum != filter.cellNum) {
            //LOGGER.error("filter's cell number not equal, left:{}, right:{}", this.cellNum, filter.cellNum);
            return null;
        } else {
            this.subtract(filter);
            List<Map<String, String>> rels = new ArrayList<>();
            Set<String> relKeys = new HashSet<>();
            Iterator<List<byte[]>> var4 = this.recover().values().iterator();

            while (var4.hasNext()) {
                List<byte[]> recoveredKeys = (List<byte[]>) var4.next();
                Iterator<byte[]> var6 = recoveredKeys.iterator();

                while (var6.hasNext()) {
                    byte[] keySum = (byte[]) var6.next();
                    Map<String, String> rel = new HashMap<>();
                    String relKey = this.parseRelFromKeySum(keySum, rel);
                    if (!relKeys.contains(relKey)) {
                        rels.add(rel);
                        relKeys.add(relKey);
                    }
                }
            }

            return rels;
        }
    }

    private Map<String, List<byte[]>> recover() {
        ArrayDeque<Integer> pure = new ArrayDeque<>();

        for (int i = 0; i < this.cellNum; ++i) {
            if (((ExtendBloomFilterCell) this.cells.get(i)).recoverAble()) {
                pure.addLast(i);
            }
        }

        List<byte[]> needToAddOrUpdateList = new ArrayList<>();
        List<byte[]> needToDelOrUpdateList = new ArrayList<>();

        while (true) {
            int integer;
            Optional<byte[]> recoveredKeySum;
            do {
                if (pure.isEmpty()) {
                    Map<String, List<byte[]>> recoveredKeys = new HashMap<>();
                    recoveredKeys.put("needToDelOrUpdate", needToDelOrUpdateList);
                    recoveredKeys.put("needToAddOrUpdate", needToAddOrUpdateList);
                    return recoveredKeys;
                }

                integer = (Integer) pure.pop();
                recoveredKeySum = ((ExtendBloomFilterCell) this.cells.get(integer)).recoverKeySum();
            } while (!recoveredKeySum.isPresent());

            if (((ExtendBloomFilterCell) this.cells.get(integer)).getCount() == 1) {
                needToDelOrUpdateList.add(recoveredKeySum.get());
            } else if (((ExtendBloomFilterCell) this.cells.get(integer)).getCount() == -1) {
                needToAddOrUpdateList.add(recoveredKeySum.get());
            }

            ArrayDeque<Integer> pureNew = this.delOneKeyAndGetPure((byte[]) recoveredKeySum.get(), integer);

            while (!pureNew.isEmpty()) {
                pure.addLast(pureNew.pop());
            }
        }
    }

    private void processDifferenceType(Set<String> needToAddOrUpdateList, Set<String> needToDelOrUpdateList) {
        Set<String> needToUpdateList = new HashSet<>();
        needToUpdateList.addAll(needToAddOrUpdateList);
        needToUpdateList.retainAll(needToDelOrUpdateList);
        this.needToUpdateIds.addAll(needToUpdateList);
        needToAddOrUpdateList.removeAll(needToUpdateList);
        this.needToAddIds.addAll(needToAddOrUpdateList);
        needToDelOrUpdateList.removeAll(needToUpdateList);
        this.needToDelIds.addAll(needToDelOrUpdateList);
    }

    public List<byte[]> calculatingDifferences(ExtendBloomFilter filter) {
        if (this.cellNum != filter.cellNum) {
            //LOGGER.error("filter's cell number not equal, left:{}, right:{}", this.cellNum, filter.cellNum);
            return null;
        } else {
            this.subtract(filter);
            List<byte[]> diffKeySums = new ArrayList<>();
            Iterator<List<byte[]>> var3 = this.recover().values().iterator();

            while (var3.hasNext()) {
                List<byte[]> recoverKeys = (List<byte[]>) var3.next();
                diffKeySums.addAll(recoverKeys);
            }

            return diffKeySums;
        }
    }

    private void subtract(ExtendBloomFilter filter) {
        for (int i = 0; i < this.cellNum; ++i) {
            ((ExtendBloomFilterCell) this.cells.get(i)).subtract((ExtendBloomFilterCell) filter.cells.get(i));
        }

    }

    private String parseResFromKeySum(byte[] keySum) {
        return keySum[0] == 1
            ? ExtendUUIDOperation.byte2UUID(keySum, 1)
            : String.valueOf(ExtendUUIDOperation.byte2Long(keySum, 1));
    }

    private String parseRelFromKeySum(byte[] keySum, Map<String, String> rel) {
        String srcId = null;
        String endId = null;
        if (keySum[0] == 1) {
            srcId = String.valueOf(ExtendUUIDOperation.byte2Long(keySum, 1));
            endId = ExtendUUIDOperation.byte2UUID(keySum, 17);
        } else if (keySum[0] == 16) {
            srcId = ExtendUUIDOperation.byte2UUID(keySum, 1);
            endId = String.valueOf(ExtendUUIDOperation.byte2Long(keySum, 17));
        } else if (keySum[0] == 0) {
            srcId = String.valueOf(ExtendUUIDOperation.byte2Long(keySum, 1));
            endId = String.valueOf(ExtendUUIDOperation.byte2Long(keySum, 17));
        } else {
            srcId = ExtendUUIDOperation.byte2UUID(keySum, 1);
            endId = ExtendUUIDOperation.byte2UUID(keySum, 17);
        }

        if (rel != null) {
            rel.put(srcId, endId);
        }

        return srcId + "_" + endId;
    }

    private ArrayDeque<Integer> delOneKeyAndGetPure(byte[] key, int cellIdx) {
        ArrayDeque<Integer> pure = new ArrayDeque<>();
        ExtendBloomFilterCell cell = ((ExtendBloomFilterCell) this.cells.get(cellIdx)).copy();
        IntStream.range(0, 3).map((hashFunIdx) -> {
            return this.hashToCell(key, hashFunIdx);
        }).forEach((idx) -> {
            ((ExtendBloomFilterCell) this.cells.get(idx)).subtract(cell);
            if (((ExtendBloomFilterCell) this.cells.get(idx)).recoverAble()) {
                pure.add(idx);
            }

        });
        return pure;
    }

    private void generateCells() {
        this.cells = new ArrayList<>(this.cellNum);

        for (int i = 0; i < this.cellNum; ++i) {
            ExtendBloomFilterCell cell = new ExtendBloomFilterCell(this.keySize);
            this.cells.add(cell);
        }

    }

    private int hashToCell(byte[] key, int hashFunIdx) {
        Integer hash = ExtendHash.hash(key, hashFunIdx);
        return (hash & Integer.MAX_VALUE) % this.offset + hashFunIdx * this.offset;
    }

    public int getCellsNum() {
        return this.cellNum;
    }

    public void setCellsNum(int cellNum) {
        this.cellNum = cellNum;
    }

    public int getNumCount() {
        return this.numCount.intValue();
    }

    public void setNumCount(int numCount) {
        this.numCount.set(numCount);
    }

    public int getCellNum() {
        return this.cellNum;
    }

    public void setCellNum(int cellNum) {
        this.cellNum = cellNum;
    }

    public int getOffset() {
        return this.offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public List<ExtendBloomFilterCell> getCells() {
        return this.cells;
    }

    public void setCells(List<ExtendBloomFilterCell> cells) {
        this.cells = cells;
    }

    public int getKeySize() {
        return this.keySize;
    }

    public void setKeySize(int keySize) {
        this.keySize = keySize;
    }
}
