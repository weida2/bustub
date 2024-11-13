#include <sstream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#define WZC_Remove_
#define POSITIVE_CRAB
namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
#ifdef WZC_Remove_
  LOG_DEBUG("leaf_max_size_: %d, internal_max_size_: %d\n", leaf_max_size_, internal_max_size_);
#endif
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_header_page = guard.template AsMut<BPlusTreeHeaderPage>();
  root_header_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  bool is_empty{false};
  ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();
  is_empty = header_page->root_page_id_ == INVALID_PAGE_ID;
  header_guard.Drop();
  return is_empty;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;
  ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
  if (header_guard.As<BPlusTreeHeaderPage>()->root_page_id_ == INVALID_PAGE_ID) {
    return false;
  }
  ReadPageGuard cur_guard = bpm_->FetchPageRead(header_guard.As<BPlusTreeHeaderPage>()->root_page_id_);
  header_guard.Drop();
  auto cur_page = cur_guard.As<BPlusTreePage>();
  while (!cur_page->IsLeafPage()) {
    int slot_num = FindInternal(key, cur_page);
    if (slot_num == -1) {
      return false;
    }
    cur_guard = bpm_->FetchPageRead(reinterpret_cast<const InternalPage *>(cur_page)->ValueAt(slot_num));
    cur_page = cur_guard.As<BPlusTreePage>();
  }
  auto leaf_page = reinterpret_cast<const LeafPage *>(cur_page);
  int slot_num = FindLeaf(key, leaf_page);
  if (slot_num != -1) {
    result->push_back(leaf_page->ValueAt(slot_num));
    return true;
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindInternal(const KeyType &key, const BPlusTreePage *bp_page) -> int {
  auto internal_page = reinterpret_cast<const InternalPage *>(bp_page);
  int st = 1;
  int ed = internal_page->GetSize() - 1;
  // assert(ed > 0);
  while (st <= ed) {
    int mid = (st + ed) / 2;
    if (comparator_(key, internal_page->KeyAt(mid)) < 0) {
      if (mid == st) {
        return st - 1;
      }
      if (comparator_(key, internal_page->KeyAt(mid - 1)) >= 0) {
        return mid - 1;
      }
      ed = mid - 1;
    } else {
      if (mid == ed) {
        return ed;
      }
      if (comparator_(key, internal_page->KeyAt(mid + 1)) < 0) {
        return mid;
      }
      st = mid + 1;
    }
  }
  return -1;  // 没找到，内部结点不会出现
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeaf(const KeyType &key, const BPlusTreePage *bp_page) -> int {
  auto leaf_page = reinterpret_cast<const LeafPage *>(bp_page);
  int st = 0;
  int ed = leaf_page->GetSize() - 1;
  // assert(ed >= 0);
  while (st <= ed) {
    int mid = (st + ed) / 2;
    int ret = comparator_(key, leaf_page->KeyAt(mid));
    if (ret == 0) {
      return mid;
    }
    if (ret > 0) {
      st = mid + 1;
    } else {
      ed = mid - 1;
    }
  }
  return -1;  // 叶子结点不存在该key
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;
  ctx.write_set_.clear();
#ifdef WZC_Remove_
  auto log = std::stringstream();
  log << "--[thread " << std::this_thread::get_id() << "] | 插入key: " << key << std::endl;
  LOG_DEBUG("%s", log.str().c_str());
#endif
  // 乐观螃蟹锁
#ifdef POSITIVE_CRAB
  MappingType tmp_ps;
  MappingType ins_ps = {key, value};
  int slot_num_ps = -1;
  int split_index_ps = 0;

  // 先获取读锁
  ReadPageGuard header_read_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_read_guard.As<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ != INVALID_PAGE_ID) {  // 如果非空
    ReadPageGuard read_guard = bpm_->FetchPageRead(header_page->root_page_id_);
    WritePageGuard leaf_guard;
    auto cur_page = read_guard.As<InternalPage>();
    if (cur_page->IsLeafPage()) {  // 只有root节点
      read_guard.Drop();
      leaf_guard = bpm_->FetchPageWrite(header_page->root_page_id_);
      header_read_guard.Drop();
    } else {
      header_read_guard.Drop();
      bool st{false};
      KeyType st_key;
      st_key.SetFromInter(1);
      while (!st) {
        int slot_num = FindInternal(key, cur_page);
        if (comparator_(cur_page->KeyAt(0), st_key) != 0) {
          read_guard = bpm_->FetchPageRead(cur_page->ValueAt(slot_num));
          cur_page = read_guard.As<InternalPage>();
        } else {
          leaf_guard = bpm_->FetchPageWrite(cur_page->ValueAt(slot_num));
          read_guard.Drop();
          st = true;
        }
      }
    }
    auto leaf_page = leaf_guard.AsMut<LeafPage>();
    if (FindLeaf(key, leaf_page) != -1) {
      return false;
    }
    // 乐观插入，不会分裂
    if (leaf_page->GetSize() < leaf_page->GetMaxSize()) {
      for (int i = 0; i < leaf_page->GetSize(); i++) {
        if (comparator_(key, leaf_page->KeyAt(i)) < 0 && slot_num_ps == -1) {
          slot_num_ps = i;
        }
        if (slot_num_ps != -1) {
          tmp_ps = {leaf_page->KeyAt(i), leaf_page->ValueAt(i)};
          leaf_page->SetAt(i, ins_ps.first, ins_ps.second);
          ins_ps = tmp_ps;
        }
      }
      leaf_page->IncreaseSize(1);
      leaf_page->SetAt(leaf_page->Size() - 1, ins_ps.first, ins_ps.second);
      return true;
    }
    // 如果分裂,就交给悲观处理
    leaf_guard.Drop();
  } else {  // 空树直接交给悲观新建root
    header_read_guard.Drop();
  }
#endif

  // 悲观螃蟹锁
  WritePageGuard header_write_guard = bpm_->FetchPageWrite(header_page_id_);
  // 如果root_page为空，建tree,为LeafPage类型
  if (header_write_guard.As<BPlusTreeHeaderPage>()->root_page_id_ == INVALID_PAGE_ID) {
    page_id_t root_page_id;
    BasicPageGuard tmp_pin_guard = bpm_->NewPageGuarded(&root_page_id);
    // 这里root_guard要获取在header_page赋值之前或者drop()之前
    // 因为如果获取在之后有可能其他线程已经获取了header_page进而获取root_page进行操作
    WritePageGuard root_guard = bpm_->FetchPageWrite(root_page_id);
    tmp_pin_guard.Drop();

    auto header_page = header_write_guard.AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = root_page_id;
    header_write_guard.Drop();

    auto *root_page = root_guard.AsMut<LeafPage>();
    root_page->Init(leaf_max_size_);
    root_page->IncreaseSize(1);
    root_page->SetAt(0, key, value);
    root_guard.Drop();
    return true;
  }

  auto header_page = header_write_guard.AsMut<BPlusTreeHeaderPage>();
  WritePageGuard root_guard = bpm_->FetchPageWrite(header_page->root_page_id_);
  auto cur_page = root_guard.As<InternalPage>();
  ctx.write_set_.push_back(std::move(header_write_guard));  // 加入header结点,判断最后分裂是否迭代到
  if (cur_page->GetSize() < cur_page->GetMaxSize()) {
    // 判断root结点是否安全，即其size是否 < max_size
    // 因为对于插入操作，如果size >= max_size, 再插入就会分裂，不安全
    // 其实判断条件和下面的非root结点判断相同
    ctx.write_set_.clear();
  }
  ctx.write_set_.push_back(std::move(root_guard));

  while (!cur_page->IsLeafPage()) {
    WritePageGuard cur_guard;
    int slot_num = FindInternal(key, cur_page);
    cur_guard = bpm_->FetchPageWrite(cur_page->ValueAt(slot_num));
    cur_page = cur_guard.As<InternalPage>();
    if (cur_page->GetSize() < cur_page->GetMaxSize()) {
      // 确定当前结点是否安全，来释放父节结点
      // 在插入操作中, 内部结点或者叶子节点(都是非root结点)安全的前提是
      // 其是不是会分裂, 也即其 size >= max_size, 这样再做插入就会分裂, 所以不安全
      // 反之安全，即 size < max_size (没到达这个阈值)
      ctx.write_set_.clear();
    }
    ctx.write_set_.push_back(std::move(cur_guard));
  }
  // 找到叶子结点
  auto leaf_guard = std::move(ctx.write_set_.back());
  auto leaf_page_nomut = leaf_guard.As<LeafPage>();
  ctx.write_set_.pop_back();

  // 处理重复key
  if (FindLeaf(key, leaf_page_nomut) != -1) {
    return false;
  }
  MappingType tmp;
  MappingType ins = {key, value};
  int slot_num = -1;
  int split_index = 0;

  auto leaf_page = leaf_guard.AsMut<LeafPage>();
  // 情况1.直接插入
  if (leaf_page->GetSize() < leaf_page->GetMaxSize()) {  // 后split操作
    // 冒泡排序、插入
    for (int i = 0; i < leaf_page->GetSize(); i++) {
      if (comparator_(key, leaf_page->KeyAt(i)) < 0 && slot_num == -1) {
        slot_num = i;
      }
      if (slot_num != -1) {
        tmp = {leaf_page->KeyAt(i), leaf_page->ValueAt(i)};
        leaf_page->SetAt(i, ins.first, ins.second);
        ins = tmp;
      }
    }
    leaf_page->IncreaseSize(1);
    leaf_page->SetAt(leaf_page->GetSize() - 1, ins.first, ins.second);
    return true;
  }

  // 情况2.叶子结点分裂
  KeyType origin_key;
  page_id_t origin_page_id = leaf_guard.PageId();
  KeyType split_key;
  page_id_t split_page_id;
  BasicPageGuard tmp_pin_guard = bpm_->NewPageGuarded(&split_page_id);
  auto split_leaf_guard = bpm_->FetchPageWrite(split_page_id);
  tmp_pin_guard.Drop();
  auto split_leaf_page = split_leaf_guard.AsMut<LeafPage>();
  split_leaf_page->Init(leaf_max_size_);
  // 向上取整,右边结点数目>=左边
  split_leaf_page->SetSize((leaf_page->GetMaxSize() + 1) - (leaf_page->GetMaxSize() + 1) / 2);
  split_index = (leaf_page->GetMaxSize() + 1) / 2;

  for (int i = 0; i < leaf_page->GetMaxSize(); i++) {
    if (comparator_(key, leaf_page->KeyAt(i)) < 0 && slot_num == -1) {
      slot_num = i;
    }
    if (slot_num != -1) {
      // 找到在排序key內部找到插入点
      // 插入左边内部
      if (i < split_index) {
        tmp = {leaf_page->KeyAt(i), leaf_page->ValueAt(i)};
        leaf_page->SetAt(i, ins.first, ins.second);
        ins = tmp;
      } else {  // 插入右边内部
        tmp = {leaf_page->KeyAt(i), leaf_page->ValueAt(i)};
        split_leaf_page->SetAt(i - split_index, ins.first, ins.second);
        ins = tmp;
      }
    } else {  // 插入最右边
      if (i >= split_index) {
        split_leaf_page->SetAt(i - split_index, leaf_page->KeyAt(i), leaf_page->ValueAt(i));
      }
    }
  }
  split_leaf_page->SetAt(split_leaf_page->GetSize() - 1, ins.first, ins.second);
  split_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(split_page_id);
  leaf_page->SetSize((leaf_page->GetMaxSize() + 1) / 2);
  origin_key = leaf_page->KeyAt(0);
  split_key = split_leaf_page->KeyAt(0);
  leaf_guard.Drop();
  split_leaf_guard.Drop();

  // 情况3.处理向上迭代(内部结点)
  page_id_t new_split_page_id;
  // 只要ctx.write_set大于1,说明叶子结点上一级父结点也满了,插入就会分裂
  while (ctx.write_set_.size() > 1) {
    BasicPageGuard tmp_pin_guard = bpm_->NewPageGuarded(&new_split_page_id);
    WritePageGuard split_inter_guard = bpm_->FetchPageWrite(new_split_page_id);
    tmp_pin_guard.Drop();
    WritePageGuard parent_inter_guard = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
    auto parent_inter_page = parent_inter_guard.AsMut<InternalPage>();
    auto split_inter_page = split_inter_guard.AsMut<InternalPage>();
    split_inter_page->Init(internal_max_size_);
    // 向下取整+1,内部结点左边>=右边(涵盖k=0结点) 0+key >= 0+key
    split_inter_page->SetSize((parent_inter_page->GetMaxSize() + 1) - (parent_inter_page->GetMaxSize() / 2 + 1));
    split_index = parent_inter_page->GetMaxSize() / 2 + 1;
    slot_num = -1;
    KeyType k_tmp;
    page_id_t p_tmp;
    KeyType k_ins = split_key;
    page_id_t p_ins = split_page_id;

    KeyType new_split_key = split_key;  // 父节点分裂,选择分裂的键上移

    for (int i = 1; i < parent_inter_page->GetMaxSize(); i++) {
      if (comparator_(split_key, parent_inter_page->KeyAt(i)) < 0 && slot_num == -1) {
        slot_num = i;
      }
      if (slot_num != -1 || i >= split_index) {
        // key插入前面
        if (i < split_index) {
          k_tmp = parent_inter_page->KeyAt(i);
          p_tmp = parent_inter_page->ValueAt(i);
          parent_inter_page->SetKeyAt(i, k_ins);
          parent_inter_page->SetValueAt(i, p_ins);
          k_ins = k_tmp, p_ins = p_tmp;
        } else if (i == split_index) {
          // key已经插入
          if (slot_num != -1) {
            new_split_key = k_ins;
            split_inter_page->SetValueAt(0, p_ins);
            k_ins = parent_inter_page->KeyAt(i);
            p_ins = parent_inter_page->ValueAt(i);
          } else {  // key还未插入
            new_split_key = parent_inter_page->KeyAt(i);
            split_inter_page->SetValueAt(0, parent_inter_page->ValueAt(i));
          }
        } else {
          if (slot_num != -1) {
            split_inter_page->SetKeyAt(i - split_index, k_ins);
            split_inter_page->SetValueAt(i - split_index, p_ins);
            k_ins = parent_inter_page->KeyAt(i);
            p_ins = parent_inter_page->ValueAt(i);
          } else {
            split_inter_page->SetKeyAt(i - split_index, parent_inter_page->KeyAt(i));
            split_inter_page->SetValueAt(i - split_index, parent_inter_page->ValueAt(i));
          }
        }
      }
    }
    parent_inter_page->SetSize(parent_inter_page->GetMaxSize() / 2 + 1);
    split_inter_page->SetKeyAt(split_inter_page->GetSize() - 1, k_ins);
    split_inter_page->SetValueAt(split_inter_page->GetSize() - 1, p_ins);

    origin_key = parent_inter_page->KeyAt(1);  // no use
    origin_page_id = parent_inter_guard.PageId();
    split_page_id = new_split_page_id;
    split_key = new_split_key;
    parent_inter_guard.Drop();
    split_inter_guard.Drop();
  }

  // 情况4.能到这一步说明header结点往下的root,internal各级都满了,需要新建root,InternalPage类型
  if (ctx.write_set_.front().PageId() == header_page_id_) {
    page_id_t root_page_id;
    BasicPageGuard tmp_pin_guard = bpm_->NewPageGuarded(&root_page_id);
    WritePageGuard root_guard = bpm_->FetchPageWrite(root_page_id);
    tmp_pin_guard.Drop();  // 在Fetch后面释放

    auto header_page = ctx.write_set_.front().AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = root_page_id;
    ctx.write_set_.front().Drop();  // 释放header结点,在获取了root_guard之后释放,原因和开头一样

    auto root_page = root_guard.AsMut<InternalPage>();
    root_page->Init(internal_max_size_);
    root_page->IncreaseSize(1);
    root_page->SetValueAt(0, origin_page_id);
    root_page->SetKeyAt(1, split_key);
    root_page->SetValueAt(1, split_page_id);
    root_guard.Drop();
    return true;
  }

  // 情况 3.1 && 4.1(分支) 向上处理一层或者几层后到头了，不再分裂
  WritePageGuard parent_inter_guard = std::move(ctx.write_set_.back());
  auto parent_inter_page = parent_inter_guard.AsMut<InternalPage>();
  ctx.write_set_.pop_back();
  KeyType k_ins = split_key;
  page_id_t p_ins = split_page_id;
  slot_num = FindInternal(split_key, parent_inter_page) + 1;
  parent_inter_page->IncreaseSize(1);
  for (int i = parent_inter_page->GetSize() - 1; i > slot_num; i--) {
    parent_inter_page->SetKeyAt(i, parent_inter_page->KeyAt(i - 1));
    parent_inter_page->SetValueAt(i, parent_inter_page->ValueAt(i - 1));
  }
  parent_inter_page->SetKeyAt(slot_num, k_ins);
  parent_inter_page->SetValueAt(slot_num, p_ins);
  parent_inter_guard.Drop();

  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  Context ctx;
  ctx.write_set_.clear();
#ifdef WZC_Remove_
  auto log = std::stringstream();
  log << "--[thread " << std::this_thread::get_id() << "] | 删除key: " << key << std::endl;
  LOG_DEBUG("%s", log.str().c_str());
  // LOG_DEBUG("删除前树的形状为:\n");
  // Print(bpm_);
#endif
  WritePageGuard header_write_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_write_guard.AsMut<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    return;
  }

  // 先找到叶子结点，同时存储路径，加锁|释放锁
  ctx.root_page_id_ = header_page->root_page_id_;
  WritePageGuard root_page_guard = bpm_->FetchPageWrite(header_page->root_page_id_);
  auto cur_page = root_page_guard.AsMut<InternalPage>();
  ctx.write_set_.push_back(std::move(header_write_guard));  // 加入ctx后此时page_guard还在，直到clear了才drop掉
  if ((cur_page->IsLeafPage() && cur_page->GetSize() >= 2) || cur_page->GetSize() >= 3) {
    // 这里对root结点判断安全条件不大相同, 因为分裂操作root结点情况稍微不同
    // 有和没有header结点即确认root结点是否安全，对于root结点安全的判断如下
    // 这里有header结点的前提是: 1.要么root结点是叶子结点, 只有一个键 key_0: 有效
    // 2.要么当前root结点是内部结点,除了只有两个键 key_0: Invalid, key_1: 有效
    ctx.write_set_.clear();
  }
  ctx.write_set_.push_back(std::move(root_page_guard));

  while (!cur_page->IsLeafPage()) {
    WritePageGuard cur_guard;
    int slot_num = FindInternal(key, cur_page);
    cur_guard = bpm_->FetchPageWrite(cur_page->ValueAt(slot_num));
    cur_page = cur_guard.AsMut<InternalPage>();
    // 确定当前结点是否安全，来释放父节结点
    // 在删除操作中, 内部结点或者叶子节点(都是非root结点)安全的前提是
    // 其是不是会合并(借取), 也即其 size <= min_size, 这样再做删除就会合并(借取), 所以不安全
    // 反之安全，即 size > min_size (没达到阈值)
    if (cur_page->GetSize() > cur_page->GetMinSize()) {
      ctx.write_set_.clear();
    }
    ctx.write_set_.push_back(std::move(cur_guard));
  }
  // 找到了叶子结点
  auto leaf_guard = std::move(ctx.write_set_.back());
  auto leaf_page = leaf_guard.AsMut<LeafPage>();
  ctx.write_set_.pop_back();

  int slot_num = -1;
  slot_num = FindLeaf(key, leaf_page);
  // 没找到，直接返回
  if (slot_num == -1) {
    return;
  }
  KeyType key_for_parent_locate = leaf_page->KeyAt(0);  // 用于定位parent结点已经被删除的key
  // 先删除叶子结点的key
  for (int i = slot_num; i < leaf_page->GetSize() - 1; i++) {
    leaf_page->SetAt(i, leaf_page->KeyAt(i + 1), leaf_page->ValueAt(i + 1));
  }
  leaf_page->IncreaseSize(-1);
  // 删除之后判断
  // 情况1.叶子结点直接删除，不用合并。或者叶子结点为根节点
  // 这里没有leaf的parent_ctx信息，所以暂时改不了leaf对应parent位置所在的key(这种情况是删除leaf[0].key会出现)
  if (leaf_page->GetSize() >= leaf_page->GetMinSize() || leaf_guard.PageId() == ctx.root_page_id_) {
    if ((leaf_guard.PageId() == ctx.root_page_id_) && (leaf_page->GetSize() == 0)) {
      leaf_guard.Drop();
      ctx.write_set_.front().AsMut<BPlusTreeHeaderPage>()->root_page_id_ = INVALID_PAGE_ID;
      ctx.write_set_.pop_back();
    }
    return;
  }

  // 情况2.处理叶子结点借或者合并(如果是合并，就需要向上迭代<情况3>)
  auto parent_guard = std::move(ctx.write_set_.back());
  auto parent_page = parent_guard.AsMut<InternalPage>();
  ctx.write_set_.pop_back();
  int parent_slot_num = -1;
  parent_slot_num = FindInternal(key, parent_page);

  WritePageGuard rsib_guard;
  WritePageGuard lsib_guard;
  if (parent_slot_num == 0) {  // 说明当前leaf结点位于最左边
    rsib_guard = bpm_->FetchPageWrite(parent_page->ValueAt(parent_slot_num + 1));
    auto rsib_page = rsib_guard.AsMut<LeafPage>();
    // 向右借
    if (rsib_page->GetSize() > rsib_page->GetMinSize()) {
      leaf_page->IncreaseSize(1);
      leaf_page->SetAt(leaf_page->GetSize() - 1, rsib_page->KeyAt(0), rsib_page->ValueAt(0));
      parent_page->SetKeyAt(parent_slot_num + 1, rsib_page->KeyAt(1));
      for (int i = 0; i < rsib_page->GetSize() - 1; i++) {
        rsib_page->SetAt(i, rsib_page->KeyAt(i + 1), rsib_page->ValueAt(i + 1));
      }
      rsib_page->IncreaseSize(-1);
      return;
    }
    // 向右合并
    int i = leaf_page->GetSize();
    int j = 0;
    leaf_page->IncreaseSize(rsib_page->GetSize());
    for (; j < rsib_page->GetSize(); j++, i++) {
      leaf_page->SetAt(i, rsib_page->KeyAt(j), rsib_page->ValueAt(j));
    }
    leaf_page->SetNextPageId(rsib_page->GetNextPageId());
    for (int i = parent_slot_num + 1; i < parent_page->GetSize() - 1; i++) {
      parent_page->SetKeyAt(i, parent_page->KeyAt(i + 1));
      parent_page->SetValueAt(i, parent_page->ValueAt(i + 1));
    }
    parent_page->IncreaseSize(-1);
    rsib_guard.Drop();
  } else if (parent_slot_num == parent_page->GetSize() - 1) {  // 说明当前leaf结点位于最右边
    lsib_guard = bpm_->FetchPageWrite(parent_page->ValueAt(parent_slot_num - 1));
    auto lsib_page = lsib_guard.AsMut<LeafPage>();
    // 向左借
    if (lsib_page->GetSize() > lsib_page->GetMinSize()) {
      leaf_page->IncreaseSize(1);
      for (int i = leaf_page->GetSize() - 1; i > 0; i--) {
        leaf_page->SetAt(i, leaf_page->KeyAt(i - 1), leaf_page->ValueAt(i - 1));
      }
      leaf_page->SetAt(0, lsib_page->KeyAt(lsib_page->GetSize() - 1), lsib_page->ValueAt(lsib_page->GetSize() - 1));
      parent_page->SetKeyAt(parent_slot_num, leaf_page->KeyAt(0));
      lsib_page->IncreaseSize(-1);
      return;
    }
    // 向左合并
    int i = lsib_page->GetSize();
    int j = 0;
    lsib_page->IncreaseSize(leaf_page->GetSize());
    for (; j < leaf_page->GetSize(); j++, i++) {
      lsib_page->SetAt(i, leaf_page->KeyAt(j), leaf_page->ValueAt(j));
    }
    lsib_page->SetNextPageId(leaf_page->GetNextPageId());
    for (int i = parent_slot_num; i < parent_page->GetSize() - 1; i++) {
      parent_page->SetKeyAt(i, parent_page->KeyAt(i + 1));
      parent_page->SetValueAt(i, parent_page->ValueAt(i + 1));
    }
    parent_page->IncreaseSize(-1);
    leaf_guard.Drop();
  } else {  // 说明当前leaf结点在中间
    rsib_guard = bpm_->FetchPageWrite(parent_page->ValueAt(parent_slot_num + 1));
    lsib_guard = bpm_->FetchPageWrite(parent_page->ValueAt(parent_slot_num - 1));
    auto rsib_page = rsib_guard.AsMut<LeafPage>();
    auto lsib_page = lsib_guard.AsMut<LeafPage>();
    // 向右借
    if (rsib_page->GetSize() > rsib_page->GetMinSize()) {
      leaf_page->IncreaseSize(1);
      leaf_page->SetAt(leaf_page->GetSize() - 1, rsib_page->KeyAt(0), rsib_page->ValueAt(0));
      parent_page->SetKeyAt(parent_slot_num + 1, rsib_page->KeyAt(1));
      for (int i = 0; i < rsib_page->GetSize() - 1; i++) {
        rsib_page->SetAt(i, rsib_page->KeyAt(i + 1), rsib_page->ValueAt(i + 1));
      }
      rsib_page->IncreaseSize(-1);
      return;
    }
    // 向左借
    if (lsib_page->GetSize() > lsib_page->GetMinSize()) {
      leaf_page->IncreaseSize(1);
      for (int i = leaf_page->GetSize() - 1; i > 0; i--) {
        leaf_page->SetAt(i, leaf_page->KeyAt(i - 1), leaf_page->ValueAt(i - 1));
      }
      leaf_page->SetAt(0, lsib_page->KeyAt(lsib_page->GetSize() - 1), lsib_page->ValueAt(lsib_page->GetSize() - 1));
      parent_page->SetKeyAt(parent_slot_num, leaf_page->KeyAt(0));
      lsib_page->IncreaseSize(-1);
      return;
    }
    // 向右合并(这里直接优先向右合并，所以不用加入向左合并的代码了)
    int i = leaf_page->GetSize();
    int j = 0;
    leaf_page->IncreaseSize(rsib_page->GetSize());
    for (; j < rsib_page->GetSize(); j++, i++) {
      leaf_page->SetAt(i, rsib_page->KeyAt(j), rsib_page->ValueAt(j));
    }
    leaf_page->SetNextPageId(rsib_page->GetNextPageId());
    for (int i = parent_slot_num + 1; i < parent_page->GetSize() - 1; i++) {
      parent_page->SetKeyAt(i, parent_page->KeyAt(i + 1));
      parent_page->SetValueAt(i, parent_page->ValueAt(i + 1));
    }
    parent_page->IncreaseSize(-1);
    rsib_guard.Drop();
  }

  // 情况3.处理向上父节点迭代删除(内部结点)
  ctx.write_set_.push_back(std::move(parent_guard));  // 放回去判断是否需要迭代
  while (ctx.write_set_.size() > 1) {  // 目前还没释放的结点:1.合并后的l_page(总在左边) #没释放也没关系没啥用
                                       // 2.在ctx里的:parent_page(cur_page),parent->parent
    WritePageGuard cur_guard = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
    WritePageGuard parent_guard = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();

    auto cur_page = cur_guard.AsMut<InternalPage>();
    auto parent_page = parent_guard.AsMut<InternalPage>();
    if (parent_guard.PageId() == header_page_id_) {  // 根节点修改
      auto header_page = reinterpret_cast<BPlusTreeHeaderPage *>(parent_page);
      header_page->root_page_id_ = cur_page->ValueAt(0);
      cur_guard.Drop();
      return;
    }
    parent_slot_num = -1;
    if (cur_page->GetSize() == 1) {  // 如果唯一的key被删了,就根据原来被删的key定位结点
      // key_for_parent_locate = key_for_parent_locate;
    } else {
      key_for_parent_locate = cur_page->KeyAt(1);
    }
    parent_slot_num = FindInternal(key_for_parent_locate, parent_page);

    // key for locate
    key_for_parent_locate = parent_page->KeyAt(1);

    WritePageGuard rsib_guard;
    WritePageGuard lsib_guard;
    if (parent_slot_num == 0) {  // 说明当前internal结点在最左
      rsib_guard = bpm_->FetchPageWrite(parent_page->ValueAt(parent_slot_num + 1));
      auto rsib_page = rsib_guard.AsMut<InternalPage>();
      // 向右internal借
      if (rsib_page->GetSize() > rsib_page->GetMinSize()) {
        cur_page->IncreaseSize(1);
        cur_page->SetKeyAt(cur_page->GetSize() - 1, parent_page->KeyAt(parent_slot_num + 1));
        cur_page->SetValueAt(cur_page->GetSize() - 1, rsib_page->ValueAt(0));
        // 向上提供key_1,因为内部结点key_0无效(key_0的key在其父节点对应槽位的key上),value有效
        parent_page->SetKeyAt(parent_slot_num + 1, rsib_page->KeyAt(1));
        for (int i = 0; i < rsib_page->GetSize() - 1; i++) {
          if (i != 0) {
            rsib_page->SetKeyAt(i, rsib_page->KeyAt(i + 1));
          }
          rsib_page->SetValueAt(i, rsib_page->ValueAt(i + 1));
        }
        rsib_page->IncreaseSize(-1);
        return;
      }
      // 向右internal合并
      int i = cur_page->GetSize();
      int j = 0;
      cur_page->IncreaseSize(rsib_page->GetSize());
      for (; j < rsib_page->GetSize(); j++, i++) {
        if (j == 0) {
          cur_page->SetKeyAt(i, parent_page->KeyAt(parent_slot_num + 1));
        } else {
          cur_page->SetKeyAt(i, rsib_page->KeyAt(j));
        }
        cur_page->SetValueAt(i, rsib_page->ValueAt(j));
      }
      for (int i = parent_slot_num + 1; i < parent_page->GetSize() - 1; i++) {
        parent_page->SetKeyAt(i, parent_page->KeyAt(i + 1));
        parent_page->SetValueAt(i, parent_page->ValueAt(i + 1));
      }
      parent_page->IncreaseSize(-1);
      rsib_guard.Drop();
    } else if (parent_slot_num == parent_page->GetSize() - 1) {  // 说明当前internal结点在最右
      lsib_guard = bpm_->FetchPageWrite(parent_page->ValueAt(parent_slot_num - 1));
      auto lsib_page = lsib_guard.AsMut<InternalPage>();
      // 向左internal借
      if (lsib_page->GetSize() > lsib_page->GetMinSize()) {
        cur_page->IncreaseSize(1);
        for (int i = cur_page->GetSize() - 1; i >= 1; i--) {
          if (i == 1) {
            cur_page->SetKeyAt(i, parent_page->KeyAt(parent_slot_num));
          } else {
            cur_page->SetKeyAt(i, cur_page->KeyAt(i - 1));
          }
          cur_page->SetValueAt(i, cur_page->ValueAt(i - 1));
        }
        cur_page->SetValueAt(0, lsib_page->ValueAt(lsib_page->GetSize() - 1));
        parent_page->SetKeyAt(parent_slot_num, lsib_page->KeyAt(lsib_page->GetSize() - 1));
        lsib_page->IncreaseSize(-1);
        return;
      }
      // 向左internal合并
      int i = lsib_page->GetSize();
      int j = 0;
      lsib_page->IncreaseSize(cur_page->GetSize());
      for (; j < cur_page->GetSize(); j++, i++) {
        if (j == 0) {
          lsib_page->SetKeyAt(i, parent_page->KeyAt(parent_slot_num));
        } else {
          lsib_page->SetKeyAt(i, cur_page->KeyAt(j));
        }
        lsib_page->SetValueAt(i, cur_page->ValueAt(j));
      }
      for (int i = parent_slot_num; i < parent_page->GetSize() - 1; i++) {
        parent_page->SetKeyAt(i, parent_page->KeyAt(i + 1));
        parent_page->SetValueAt(i, parent_page->ValueAt(i + 1));
      }
      parent_page->IncreaseSize(-1);
      cur_guard.Drop();
    } else {  // 说明当前internal在中间
      rsib_guard = bpm_->FetchPageWrite(parent_page->ValueAt(parent_slot_num + 1));
      lsib_guard = bpm_->FetchPageWrite(parent_page->ValueAt(parent_slot_num - 1));
      auto rsib_page = rsib_guard.AsMut<InternalPage>();
      auto lsib_page = lsib_guard.AsMut<InternalPage>();
      // 向右internal借
      if (rsib_page->GetSize() > rsib_page->GetMinSize()) {
        cur_page->IncreaseSize(1);
        cur_page->SetKeyAt(cur_page->GetSize() - 1, parent_page->KeyAt(parent_slot_num + 1));
        cur_page->SetValueAt(cur_page->GetSize() - 1, rsib_page->ValueAt(0));
        // 向上提供key_1,因为内部结点key_0无效(key_0的key在其父节点对应槽位的key上),value有效
        parent_page->SetKeyAt(parent_slot_num + 1, rsib_page->KeyAt(1));
        for (int i = 0; i < rsib_page->GetSize() - 1; i++) {
          if (i != 0) {
            rsib_page->SetKeyAt(i, rsib_page->KeyAt(i + 1));
          }
          rsib_page->SetValueAt(i, rsib_page->ValueAt(i + 1));
        }
        rsib_page->IncreaseSize(-1);
        return;
      }
      // 向左internal借
      if (lsib_page->GetSize() > lsib_page->GetMinSize()) {
        cur_page->IncreaseSize(1);
        for (int i = cur_page->GetSize() - 1; i >= 1; i--) {
          if (i == 1) {
            cur_page->SetKeyAt(i, parent_page->KeyAt(parent_slot_num));
          } else {
            cur_page->SetKeyAt(i, cur_page->KeyAt(i - 1));
          }
          cur_page->SetValueAt(i, cur_page->ValueAt(i - 1));
        }
        cur_page->SetValueAt(0, lsib_page->ValueAt(lsib_page->GetSize() - 1));
        parent_page->SetKeyAt(parent_slot_num, lsib_page->KeyAt(lsib_page->GetSize() - 1));
        lsib_page->IncreaseSize(-1);
        return;
      }
      // 向右internal合并
      int i = cur_page->GetSize();
      int j = 0;
      cur_page->IncreaseSize(rsib_page->GetSize());
      for (; j < rsib_page->GetSize(); j++, i++) {
        if (j == 0) {
          cur_page->SetKeyAt(i, parent_page->KeyAt(parent_slot_num + 1));
        } else {
          cur_page->SetKeyAt(i, rsib_page->KeyAt(j));
        }
        cur_page->SetValueAt(i, rsib_page->ValueAt(j));
      }
      for (int i = parent_slot_num + 1; i < parent_page->GetSize() - 1; i++) {
        parent_page->SetKeyAt(i, parent_page->KeyAt(i + 1));
        parent_page->SetValueAt(i, parent_page->ValueAt(i + 1));
      }
      parent_page->IncreaseSize(-1);
      rsib_guard.Drop();
    }
    ctx.write_set_.push_back(std::move(parent_guard));
  }
  ctx.write_set_.front().Drop();  // 最后drop header 或者 其他到顶的非header结点
  // return;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BeginFindLeaf(const KeyType &key, const LeafPage *leaf_page) -> int {
  int l = 0;
  int r = leaf_page->GetSize() - 1;
  while (l < r) {
    int mid = (l + r + 1) >> 1;
    if (comparator_(leaf_page->KeyAt(mid), key) != 1) {
      l = mid;
    } else {
      r = mid - 1;
    }
  }

  if (r >= 0 && comparator_(leaf_page->KeyAt(r), key) == 1) {
    r = -1;
  }

  return r;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BeginFindInternal(const KeyType &key, const InternalPage *internal_page) -> int {
  int l = 1;
  int r = internal_page->GetSize() - 1;
  while (l < r) {
    int mid = (l + r + 1) >> 1;
    if (comparator_(internal_page->KeyAt(mid), key) != 1) {
      l = mid;
    } else {
      r = mid - 1;
    }
  }

  if (r == -1 || comparator_(internal_page->KeyAt(r), key) == 1) {
    r = 0;
  }

  return r;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  ReadPageGuard header_page_guard = bpm_->FetchPageRead(header_page_id_);
  if (header_page_guard.As<BPlusTreeHeaderPage>()->root_page_id_ == INVALID_PAGE_ID) {
    return End();
  }
  ReadPageGuard cur_page_guard = bpm_->FetchPageRead(header_page_guard.As<BPlusTreeHeaderPage>()->root_page_id_);
  auto cur_page = cur_page_guard.As<BPlusTreePage>();
  header_page_guard.Drop();

  while (!cur_page->IsLeafPage()) {
    int slot_num = 0;
    cur_page_guard = bpm_->FetchPageRead(reinterpret_cast<const InternalPage *>(cur_page)->ValueAt(slot_num));
    cur_page = cur_page_guard.As<BPlusTreePage>();
  }

  return INDEXITERATOR_TYPE(bpm_, cur_page_guard.PageId(), 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  ReadPageGuard header_page_guard = bpm_->FetchPageRead(header_page_id_);
  if (header_page_guard.As<BPlusTreeHeaderPage>()->root_page_id_ == INVALID_PAGE_ID) {
    return End();
  }
  auto cur_page_guard = bpm_->FetchPageRead(header_page_guard.As<BPlusTreeHeaderPage>()->root_page_id_);
  auto cur_page = cur_page_guard.As<BPlusTreePage>();
  header_page_guard.Drop();
  int slot_num = -1;
  if (!cur_page->IsLeafPage()) {
    auto inter_page = reinterpret_cast<const InternalPage *>(cur_page);
    slot_num = BeginFindInternal(key, inter_page);
    if (slot_num == -1) {
      BUSTUB_ENSURE(1 == 2, "iterator begin(key) invalid in internal page");
      return End();
    }
    cur_page_guard = bpm_->FetchPageRead(inter_page->ValueAt(slot_num));
    cur_page = cur_page_guard.As<BPlusTreePage>();
  }
  auto leaf_page = reinterpret_cast<const LeafPage *>(cur_page);
  slot_num = BeginFindLeaf(key, leaf_page);
  if (slot_num == -1) {
    BUSTUB_ENSURE(1 == 2, "iterator begin(key) not find leaf page");
    return End();
  }
  return INDEXITERATOR_TYPE(bpm_, cur_page_guard.PageId(), slot_num);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(bpm_, INVALID_PAGE_ID, -1); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  auto root_header_page = guard.As<BPlusTreeHeaderPage>();
  page_id_t root_page_id = root_header_page->root_page_id_;
  guard.Drop();
  return root_page_id;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

/*
 * This method is used for test only
 * Read data from file and insert/remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BatchOpsFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  char instruction;
  std::ifstream input(file_name);
  while (input) {
    input >> instruction >> key;
    RID rid(key);
    KeyType index_key;
    index_key.SetFromInteger(key);
    switch (instruction) {
      case 'i':
        Insert(index_key, rid, txn);
        break;
      case 'd':
        Remove(index_key, txn);
        break;
      default:
        break;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
