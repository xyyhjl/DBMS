/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_file_handle.h"

/**
 * @description: 获取当前表中记录号为rid的记录
 * @param {Rid&} rid 记录号，指定记录的位置
 * @param {Context*} context
 * @return {unique_ptr<RmRecord>} rid对应的记录对象指针
 */
std::unique_ptr<RmRecord> RmFileHandle::get_record(const Rid& rid, Context* context) const {
    // Todo:
    // 1. 获取指定记录所在的page handle
    // 2. 初始化一个指向RmRecord的指针（赋值其内部的data和size）

    RmPageHandle page_handle = fetch_page_handle(rid.page_no);  
    // 获取指定槽的记录数据  
    char* record_data = page_handle.get_slot(rid.slot_no);  
    
    // 创建RmRecord对象，并使用record_data和大小进行初始化  
    std::unique_ptr<RmRecord> record_ptr = std::make_unique<RmRecord>(file_hdr_.record_size, record_data);  
    
    return record_ptr; // 返回unique_ptr<RmRecord>  
}

/**
 * @description: 在当前表中插入一条记录，不指定插入位置
 * @param {char*} buf 要插入的记录的数据
 * @param {Context*} context
 * @return {Rid} 插入的记录的记录号（位置）
 */
Rid RmFileHandle::insert_record(char* buf, Context* context) {
    // Todo:
    // 1. 获取当前未满的page handle
    // 2. 在page handle中找到空闲slot位置
    // 3. 将buf复制到空闲slot位置
    // 4. 更新page_handle.page_hdr中的数据结构
    // 注意考虑插入一条记录后页面已满的情况，需要更新file_hdr_.first_free_page_no

    RmPageHandle page_handle = create_page_handle();  
    int slot_no = -1;
    for (int i = 0; i < page_handle.file_hdr->bitmap_size; i++) {
        if (!Bitmap::is_set(page_handle.bitmap, i)) {
            slot_no = i;
            break;
        }
    } // 查找空闲槽位  
    if (slot_no == -1) {  
        // 如果没有空闲槽，就创建新页  
        page_handle = create_new_page_handle();  
        slot_no = 0; // 插入到第一个槽  
    }  

    // 插入记录  
    memcpy(page_handle.get_slot(slot_no), buf, file_hdr_.record_size);  
    Bitmap::set(page_handle.bitmap, slot_no); // 更新bitmap  
    page_handle.page_hdr->num_records++;  

    // 更新文件头的空闲页信息  
    if (page_handle.page_hdr->num_records == page_handle.file_hdr->num_records_per_page) {  
        // 页面已满，更新file_hdr的第一个空闲页  
        file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;  
    }  

    return Rid{page_handle.page->get_page_id().page_no, slot_no}; // 返回记录的RID  
}

/**
 * @description: 在当前表中的指定位置插入一条记录
 * @param {Rid&} rid 要插入记录的位置
 * @param {char*} buf 要插入记录的数据
 */
void RmFileHandle::insert_record(const Rid& rid, char* buf) {
     // 首先获取指定页面的RmPageHandle  
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);  
    
    // 检查所指定的slot是否已经被使用  
    if (Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {  
        throw std::runtime_error("Slot is already occupied. Cannot insert record.");  
    }  
    
    // 将传入的记录数据复制到指定的slot  
    memcpy(page_handle.get_slot(rid.slot_no), buf, file_hdr_.record_size);  
    
    // 更新bitmap，标记这个slot已被使用  
    Bitmap::set(page_handle.bitmap, rid.slot_no);  
    
    // 更新页面头中的槽计数  
    page_handle.page_hdr->num_records++;  
    
    // 检查是否现在页面已满，如果已满则需要更新文件头中的first_free_page_no  
    if (page_handle.page_hdr->num_records == page_handle.file_hdr->num_records_per_page) {  
        // 保存当前page_handle的下一个空闲页编号到文件头  
        file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;  
    }  
}

/**
 * @description: 删除记录文件中记录号为rid的记录
 * @param {Rid&} rid 要删除的记录的记录号（位置）
 * @param {Context*} context
 */
void RmFileHandle::delete_record(const Rid& rid, Context* context) {
    // Todo:
    // 1. 获取指定记录所在的page handle
    // 2. 更新page_handle.page_hdr中的数据结构
    // 注意考虑删除一条记录后页面未满的情况，需要调用release_page_handle()

    RmPageHandle page_handle = fetch_page_handle(rid.page_no);  
    Bitmap::reset(page_handle.bitmap, rid.slot_no); // 标记槽为未用  
    page_handle.page_hdr->num_records--;  

    // 检查是否从满变为未满  
    if (page_handle.page_hdr->num_records < page_handle.file_hdr->num_records_per_page) {  
        release_page_handle(page_handle);  
    } 
}


/**
 * @description: 更新记录文件中记录号为rid的记录
 * @param {Rid&} rid 要更新的记录的记录号（位置）
 * @param {char*} buf 新记录的数据
 * @param {Context*} context
 */
void RmFileHandle::update_record(const Rid& rid, char* buf, Context* context) {
    // Todo:
    // 1. 获取指定记录所在的page handle
    // 2. 更新记录

    RmPageHandle page_handle = fetch_page_handle(rid.page_no);  
    memcpy(page_handle.get_slot(rid.slot_no), buf, file_hdr_.record_size); // 更新记录  
}

/**
 * 以下函数为辅助函数，仅提供参考，可以选择完成如下函数，也可以删除如下函数，在单元测试中不涉及如下函数接口的直接调用
*/
/**
 * @description: 获取指定页面的页面句柄
 * @param {int} page_no 页面号
 * @return {RmPageHandle} 指定页面的句柄
 */
RmPageHandle RmFileHandle::fetch_page_handle(int page_no) const {
    // Todo:
    // 使用缓冲池获取指定页面，并生成page_handle返回给上层
    // if page_no is invalid, throw PageNotExistError exception
    if (page_no == INVALID_PAGE_ID) {  
        throw PageNotExistError("table_name", page_no);
    }  
    PageId page_id;
    page_id.page_no = page_no;
    page_id.fd = fd_;
    Page *page = buffer_pool_manager_->fetch_page(page_id);  
    return RmPageHandle(&file_hdr_, page);  
}

/**
 * @description: 创建一个新的page handle
 * @return {RmPageHandle} 新的PageHandle
 */
RmPageHandle RmFileHandle::create_new_page_handle() {
    // Todo:
    // 1.使用缓冲池来创建一个新page
    // 2.更新page handle中的相关信息
    // 3.更新file_hdr_

    PageId new_page_id;
    new_page_id.fd = this->fd_;
    // 使用缓冲池来创建一个新page
    Page* new_page = this->buffer_pool_manager_->new_page(&new_page_id);
    // 创建新的页面句柄
    RmPageHandle new_page_handle(&file_hdr_, new_page); // 创建一个新的page handle

    new_page_handle.page_hdr->next_free_page_no = -1; // 新页面的下一个空闲页无效 
    new_page_handle.page_hdr->num_records = 0;  // 新页面的记录为0
    // 更新file_hdr_
    file_hdr_.num_pages++;
    file_hdr_.first_free_page_no = new_page_id.page_no;

    return new_page_handle;
}

/**
 * @brief 创建或获取一个空闲的page handle
 *
 * @return RmPageHandle 返回生成的空闲page handle
 * @note pin the page, remember to unpin it outside!
 */
RmPageHandle RmFileHandle::create_page_handle() {
    // Todo:
    // 1. 判断file_hdr_中是否还有空闲页
    //     1.1 没有空闲页：使用缓冲池来创建一个新page；可直接调用create_new_page_handle()
    //     1.2 有空闲页：直接获取第一个空闲页
    // 2. 生成page handle并返回给上层

    if (file_hdr_.first_free_page_no != -1) {  
        // 如果存在空闲页  
        return fetch_page_handle(file_hdr_.first_free_page_no);  
    } else {  
        // 没有空闲页，创建新页  
        return create_new_page_handle();  
    }  
}

/**
 * @description: 当一个页面从没有空闲空间的状态变为有空闲空间状态时，更新文件头和页头中空闲页面相关的元数据
 */
void RmFileHandle::release_page_handle(RmPageHandle&page_handle) {
    // Todo:
    // 当page从已满变成未满，考虑如何更新：
    // 1. page_handle.page_hdr->next_free_page_no
    // 2. file_hdr_.first_free_page_no
    if (page_handle.page_hdr->num_records < page_handle.file_hdr->num_records_per_page) {  
        // 页面从已满变为未满  
        // 更新文件头和页头  
        page_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;  
        file_hdr_.first_free_page_no = page_handle.page->get_page_id().page_no; // 更新到当前页面  
    }  
}