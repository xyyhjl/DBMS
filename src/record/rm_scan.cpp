/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_scan.h"
#include "rm_file_handle.h"

/**
 * @brief 初始化file_handle和rid
 * @param file_handle
 */
RmScan::RmScan(const RmFileHandle *file_handle) : file_handle_(file_handle) {
    // Todo:
    // 初始化file_handle和rid（指向第一个存放了记录的位置）
    this->file_handle_ = file_handle;
    
    rid_.page_no = RM_FIRST_RECORD_PAGE;
    rid_.slot_no = -1;  // 索引从0开始
    // 调用next函数，找到第一个存放了记录的非空闲位置
    next();
}

/**
 * @brief 找到文件中下一个存放了记录的位置
 */
void RmScan::next() {
    // Todo:
    // 找到文件中下一个存放了记录的非空闲位置，用rid_来指向这个位置
    
    // 如果已经到达文件末尾，直接返回
    if (is_end()) {
        return;
    }
    while (true) {
         // 获取当前页的句柄
        RmPageHandle page_handle = file_handle_->fetch_page_handle(rid_.page_no);

        // 检查当前页的总槽数
        int total_slots = file_handle_->file_hdr_.num_records_per_page;

        // 在当前页面中查找有效记录
        rid_.slot_no = Bitmap::next_bit(true, page_handle.bitmap, total_slots, rid_.slot_no);
        if (rid_.slot_no < total_slots) {
            // 找到一个有效记录
            return;
        }

        // 检查是否达到文件末尾
        if ((rid_.page_no+1) >= page_handle.file_hdr->num_pages) {
            rid_.page_no = RM_NO_PAGE; // 设置文件末尾标记
            rid_.slot_no = -1;
            return;
        }

        // 如果当前页面没有有效记录，转到下一页
        rid_.page_no++;
        rid_.slot_no = -1;
    }
}

/**
 * @brief ​ 判断是否到达文件末尾
 */
bool RmScan::is_end() const {
    // Todo: 修改返回值
    return rid_.page_no == RM_NO_PAGE; // RM_NO_PAGE是文件末尾的标识
}

/**
 * @brief RmScan内部存放的rid
 */
Rid RmScan::rid() const {
    return this->rid_;
}