import frappe, erpnext
from frappe import _
from frappe.utils import cint, getdate, get_time, today
from erpnext.stock.stock_ledger import update_entries_after
from erpnext.accounts.utils import update_gl_entries_after, check_if_stock_and_account_balance_synced
from erpnext.stock.stock_ledger import get_previous_sle, get_previous_sle_fix
from erpnext.stock.stock_ledger import repost_future_sle
import timeit

def execute():
	for doctype in ('repost_item_valuation', 'stock_entry_detail', 'purchase_receipt_item',
			'purchase_invoice_item', 'delivery_note_item', 'sales_invoice_item', 'packed_item'):
		frappe.reload_doc('stock', 'doctype', doctype)
	frappe.reload_doc('buying', 'doctype', 'purchase_receipt_item_supplied')

	reposting_project_deployed_on = get_creation_time()
	posting_date = getdate(reposting_project_deployed_on)
	posting_time = get_time(reposting_project_deployed_on)

	if posting_date == today():
		return

	frappe.clear_cache()
	frappe.flags.warehouse_account_map = {}

	data = frappe.db.sql('''
		SELECT
			name, item_code, warehouse, voucher_type, voucher_no, actual_qty, voucher_detail_no, valuation_rate, incoming_rate, posting_date,  qty_after_transaction, posting_time
		FROM
			`tabStock Ledger Entry`
		WHERE
			creation >= %s
		ORDER BY timestamp(posting_date, posting_time) asc, creation asc
	''', reposting_project_deployed_on, as_dict=1)
	
	frappe.db.auto_commit_on_many_writes = 1
	print("Reposting Stock Ledger Entries...")
	total_sle = len(data)
	i = 0
	
	frappe.db.auto_commit_on_many_writes = 1
	for d in data:
		update_entries_after({
			"item_code": d.item_code,
			"warehouse": d.warehouse,
			"posting_date": d.posting_date,
			"posting_time": d.posting_time,
			"voucher_type": d.voucher_type,
			"voucher_no": d.voucher_no,
			"sle_id": d.name
		}, allow_negative_stock=True)

		i += 1
		if i%100 == 0:
			print(i, "/", total_sle)


	print("Reposting General Ledger Entries...")

	for row in frappe.get_all('Company', filters= {'enable_perpetual_inventory': 1}):
		update_gl_entries_after(posting_date, posting_time, company=row.name)

	frappe.db.auto_commit_on_many_writes = 0
	
def fix_return_sle():
	for doctype in ('repost_item_valuation', 'stock_entry_detail', 'purchase_receipt_item',
			'purchase_invoice_item', 'delivery_note_item', 'sales_invoice_item', 'packed_item'):
		frappe.reload_doc('stock', 'doctype', doctype)
	frappe.reload_doc('buying', 'doctype', 'purchase_receipt_item_supplied')

	reposting_project_deployed_on = get_creation_time()
	posting_date = getdate(reposting_project_deployed_on)
	posting_time = get_time(reposting_project_deployed_on)

	if posting_date == today():
		return

	frappe.clear_cache()
	frappe.flags.warehouse_account_map = {}

	data = frappe.db.sql('''
		SELECT
			name, item_code, warehouse, voucher_type, voucher_no, actual_qty, voucher_detail_no, valuation_rate, incoming_rate, posting_date,  qty_after_transaction, posting_time
		FROM
			`tabStock Ledger Entry`
		WHERE
			creation >= %s
		AND is_cancelled = 0
		ORDER BY timestamp(posting_date, posting_time) asc, creation asc
	''', reposting_project_deployed_on, as_dict=1)
	
	frappe.db.auto_commit_on_many_writes = 1
	print("Reposting Stock Ledger Entries...")
	total_sle = len(data)
	i = 0
	
	frappe.db.auto_commit_on_many_writes = 1
	
	for d in data:
		if d.voucher_type == "Sales Invoice":
			doc = frappe.get_doc('Sales Invoice', d.voucher_no)
			if doc.is_return:
				prev_sle = get_previous_sle_fix({
					"item_code": d.item_code,
					"warehouse": d.warehouse,
					"posting_date": d.posting_date,
					"posting_time":  d.posting_time
				})
				# if d.voucher_no == "ACC-SINV-RTN-2021-00017":
				# print(prev_sle)
				
				# print(prev_sle.valuation_rate)
				
				# if False:
				# prev_sle_doc = frappe.get_doc("Stock Ledger Entry", prev_sle.name)
				if not prev_sle == frappe._dict({}):
					print(' - '.join([d.name, doc.name, prev_sle.item_code, str(prev_sle.incoming_rate), str(prev_sle.valuation_rate)]))
					frappe.db.sql(""" UPDATE `tabStock Ledger Entry` SET recalculate_rate = 0 WHERE name = '{0}' """.format(d.name))
					frappe.db.sql(""" UPDATE `tabStock Ledger Entry` SET incoming_rate = {0} WHERE name = '{1}' """.format(prev_sle.incoming_rate, d.name))
					frappe.db.sql(""" UPDATE `tabSales Invoice Item` SET incoming_rate = {0} WHERE name = '{1}' """.format(prev_sle.incoming_rate, d.voucher_detail_no))
					frappe.db.sql(""" UPDATE `tabStock Ledger Entry` SET valuation_rate = {0} WHERE name = '{1}' """.format(prev_sle.valuation_rate, d.name))
					frappe.db.set_value('Stock Ledger Entry', d.name, 'posting_date', prev_sle.posting_date)
					frappe.db.set_value('Stock Ledger Entry', d.name, 'posting_time', prev_sle.posting_time)
					frappe.db.set_value('Sales Invoice', d.voucher_no, 'posting_date', prev_sle.posting_date)
					frappe.db.set_value('Sales Invoice', d.voucher_no, 'posting_time', prev_sle.posting_time)
					frappe.db.set_value('Sales Invoice', d.voucher_no, 'return_against', prev_sle.voucher_no)
					frappe.db.commit()
					new_doc = frappe.get_doc("Stock Ledger Entry", d.name) 
					repost_sl_entries_for_voucher(new_doc)
				
				elif prev_sle == frappe._dict({}):
					prev_sle = get_previous_sle({
						"item_code": d.item_code,
						"warehouse": d.warehouse,
						"posting_date": d.posting_date,
						"posting_time":  d.posting_time
					})
					print(' - '.join([d.name, doc.name, prev_sle.item_code, str(prev_sle.incoming_rate), str(prev_sle.valuation_rate)]))
					frappe.db.sql(""" UPDATE `tabStock Ledger Entry` SET recalculate_rate = 0 WHERE name = '{0}' """.format(d.name))
					frappe.db.sql(""" UPDATE `tabStock Ledger Entry` SET incoming_rate = {0} WHERE name = '{1}' """.format(prev_sle.valuation_rate, d.name))
					frappe.db.sql(""" UPDATE `tabSales Invoice Item` SET incoming_rate = {0} WHERE name = '{1}' """.format(prev_sle.valuation_rate, d.voucher_detail_no))
					frappe.db.sql(""" UPDATE `tabStock Ledger Entry` SET valuation_rate = {0} WHERE name = '{1}' """.format(prev_sle.valuation_rate, d.name))
					frappe.db.set_value('Stock Ledger Entry', d.name, 'posting_date', prev_sle.posting_date)
					frappe.db.set_value('Stock Ledger Entry', d.name, 'posting_time', prev_sle.posting_time)
					frappe.db.set_value('Sales Invoice', d.voucher_no, 'posting_date', prev_sle.posting_date)
					frappe.db.set_value('Sales Invoice', d.voucher_no, 'posting_time', prev_sle.posting_time)
					frappe.db.commit()
					new_doc = frappe.get_doc("Stock Ledger Entry", d.name) 
					repost_sl_entries_for_voucher(new_doc)
					# print(' - '.join([d.name, doc.name, d.item_code, 'No PREV SLE']))

				# repost_gl_entries_for_voucher(new_doc)

				i += 1
				if i%100 == 0:
					print(i, "/", total_sle)


	print("Reposting General Ledger Entries...")

	for row in frappe.get_all('Company', filters= {'enable_perpetual_inventory': 1}):
		update_gl_entries_after(posting_date, posting_time, company=row.name)

	frappe.db.auto_commit_on_many_writes = 0
	
	

def get_creation_time():
	return "2020-01-01 00:00:00"  # TimoTrade
	# return "2021-02-07 00:00:00"  # COMO
	# return "2021-02-07 23:32:07.358156"  # COMO
		# return frappe.db.sql(''' SELECT create_time FROM
	# 	INFORMATION_SCHEMA.TABLES where TABLE_NAME = "tabRepost Item Valuation" ''', as_list=1)[0][0]

def repost_sl_entries(doc):
	if doc.based_on == 'Transaction':
		repost_future_sle(voucher_type=doc.voucher_type, voucher_no=doc.voucher_no,
			allow_negative_stock=0, via_landed_cost_voucher=0)
	else:
		repost_future_sle(args=[frappe._dict({
			"item_code": doc.item_code,
			"warehouse": doc.warehouse,
			"posting_date": doc.posting_date,
			"posting_time": doc.posting_time
		})], allow_negative_stock=doc.allow_negative_stock, via_landed_cost_voucher=doc.via_landed_cost_voucher)

def repost_gl_entries(doc):
	if not cint(erpnext.is_perpetual_inventory_enabled(doc.company)):
		return

	if doc.based_on == 'Transaction':
		ref_doc = frappe.get_doc(doc.voucher_type, doc.voucher_no)
		items, warehouses = ref_doc.get_items_and_warehouses()
	else:
		items = [doc.item_code]
		warehouses = [doc.warehouse]

	update_gl_entries_after(doc.posting_date, doc.posting_time,
		warehouses, items, company=doc.company)


def repost_sl_entries_for_voucher(doc):
	# if doc.based_on == 'Transaction':
	# 	repost_future_sle(voucher_type=doc.voucher_type, voucher_no=doc.voucher_no,
	# 		allow_negative_stock=0, via_landed_cost_voucher=0)
	# else:
	repost_future_sle(args=[frappe._dict({
		"item_code": doc.item_code,
		"warehouse": doc.warehouse,
		"posting_date": doc.posting_date,
		"posting_time": doc.posting_time
	})], allow_negative_stock=0, via_landed_cost_voucher=0)

def repost_gl_entries_for_voucher(doc):
	if not cint(erpnext.is_perpetual_inventory_enabled(doc.company)):
		return

	# if doc.based_on == 'Transaction':
	ref_doc = frappe.get_doc(doc.voucher_type, doc.voucher_no)
	items, warehouses = ref_doc.get_items_and_warehouses()
	# else:
	# 	items = [doc.item_code]
	# 	warehouses = [doc.warehouse]

	update_gl_entries_after(doc.posting_date, doc.posting_time,
		warehouses, items, company=doc.company)


def fix_cancelled_values():
	# copy_data = sorted(data.copy(), key=lambda x: str(x.creation))
	data = frappe.db.sql('''
		SELECT
			name, item_code, warehouse, voucher_type, voucher_no, actual_qty, voucher_detail_no, valuation_rate, incoming_rate, posting_date,  qty_after_transaction, posting_time
		FROM
			`tabStock Ledger Entry`
		WHERE
			creation >= %s
		AND is_cancelled = 1
		ORDER BY timestamp(posting_date, posting_time) asc, creation asc
	''', "2020-01-01 00:00:00", as_dict=1)
	copy_data = data.copy()
	for d in data:
		for e in copy_data:
			if e.voucher_type == "Sales Invoice" and e.actual_qty == (-1 *d.actual_qty) and d.voucher_no == e.voucher_no and e.item_code == d.item_code and d.voucher_detail_no == e.voucher_detail_no and e.name != d.name:
				# print(e.name, d.name)
				# print(e.voucher_no, d.voucher_no)
				# print(e.item_code, d.item_code)
				# print(e.valuation_rate, d.valuation_rate)
				# frappe.db.sql(""" UPDATE `tabStock Ledger Entry` SET stock_value_difference = {0} WHERE name = '{1}' """.format(d.valuation_rate * d.actual_qty, d.name))
				# frappe.db.sql(""" UPDATE `tabStock Ledger Entry` SET valuation_rate = {0} WHERE name = '{1}' """.format(d.valuation_rate, e.name))
				# frappe.db.sql(""" UPDATE `tabStock Ledger Entry` SET incoming_rate = {0} WHERE name = '{1}' """.format(d.incoming_rate, e.name))
				if e.stock_value == 0.0:
					print(' - '.join([d.name,'updated', str(d.valuation_rate)]))
					frappe.db.sql(""" UPDATE `tabStock Ledger Entry` SET stock_value = {0} WHERE name = '{1}' """.format(e.valuation_rate * e.qty_after_transaction, e.name))
			
				frappe.db.commit()
				
				print(' - '.join([e.name,'updated', str(d.valuation_rate)]))
				print('====')
				break

def fix_return_sle_2(data):
	for d in data:
		if d.voucher_type == "Stock Entry":
			doc = frappe.get_doc('Stock Entry', d.voucher_no)

		if d.voucher_type == "Sales Invoice":
			doc = frappe.get_doc('Sales Invoice', d.voucher_no)
			if doc.is_return:
				prev_sle = get_previous_sle({
					"item_code": d.item_code,
					"warehouse": d.warehouse,
					"posting_date": d.posting_date,
					"posting_time":  d.posting_time
				})
				
				print(prev_sle.valuation_rate)
				
				print(' - '.join([d.name, doc.name, prev_sle.item_code]))
				frappe.db.sql(""" UPDATE `tabStock Ledger Entry` SET incoming_rate = {0} WHERE name = '{1}' """.format(prev_sle.valuation_rate, d.name))
				frappe.db.sql(""" UPDATE `tabSales Invoice Item` SET incoming_rate = {0} WHERE name = '{1}' """.format(prev_sle.valuation_rate, d.voucher_detail_no))
				frappe.db.sql(""" UPDATE `tabStock Ledger Entry` SET valuation_rate = {0} WHERE name = '{1}' """.format(prev_sle.valuation_rate, d.name))
				frappe.db.sql(""" UPDATE `tabStock Ledger Entry` SET recalculate_rate = 0 WHERE name = '{0}' """.format(d.name))
				frappe.db.commit()
				# new_doc = frappe.get_doc("Stock Ledger Entry", d.name) 
				# repost_sl_entries_for_voucher(new_doc)
				# repost_gl_entries_for_voucher(new_doc)


def run_reposts_manually():
	frappe.db.auto_commit_on_many_writes = 1
	for doctype in ('repost_item_valuation', 'stock_entry_detail', 'purchase_receipt_item',
			'purchase_invoice_item', 'delivery_note_item', 'sales_invoice_item', 'packed_item'):
		frappe.reload_doc('stock', 'doctype', doctype)
	frappe.reload_doc('buying', 'doctype', 'purchase_receipt_item_supplied')

	data = frappe.db.sql('''
		SELECT
			*
		FROM
			`tabRepost Item Valuation`
		WHERE status != "Completed" and posting_date >= "2021-01-01" and docstatus = 1
		ORDER BY timestamp(posting_date, posting_time) asc
	''', as_dict=1)
	x = 0
	start  = timeit.timeit()
	for d in data:
		x += 1
		print(' - '.join(['Reposting', d.name, str(d.posting_date), d.voucher_no]))
		repost(frappe.get_doc('Repost Item Valuation', d.name))
		time = start - timeit.timeit()
		print(x, time)

	frappe.db.auto_commit_on_many_writes = 0

def repost_voucher(voucher_type="Sales Invoice", voucher_no="ACC-SINV-2021-00438"):
	doc = frappe.get_doc(voucher_type, voucher_no)
	repost_sl_entries_for_voucher(doc)
	repost_gl_entries_for_voucher(doc)


def repost(doc):
	if not frappe.db.exists("Repost Item Valuation", doc.name):
		return

	doc.set_status('In Progress')
	frappe.db.commit()

	repost_sl_entries(doc)
	repost_gl_entries(doc)
	
	try:
		check_if_stock_and_account_balance_synced(doc.posting_date, doc.company)

		doc.set_status('Completed')
	except Exception:
		frappe.db.rollback()
		traceback = frappe.get_traceback()
		frappe.log_error(traceback)

		message = frappe.message_log.pop()
		if traceback:
			message += "<br>" + "Traceback: <br>" + traceback
		frappe.db.set_value(doc.doctype, doc.name, 'error_log', message)
		print(message)
		# notify_error_to_stock_managers(doc, message)
		doc.set_status('Failed')
		raise
	finally:
		frappe.db.commit()



def delete_duplicate_reposts():

	duplicate_records_list = get_duplicate_records()
	delete_duplicate_repost_entries(duplicate_records_list)

def get_duplicate_records():
	"""Fetch all but one duplicate records from the list of expired leave allocation."""
	return frappe.db.sql("""
	SELECT name, voucher_no, posting_date, count(voucher_no) as nos
		FROM `tabRepost Item Valuation`
		WHERE
		 docstatus = 1
		GROUP BY
		voucher_no
		ORDER BY
			posting_date
	""", as_dict=1)

def delete_duplicate_repost_entries(duplicate_records_list):
	"""Delete duplicate leave ledger entries."""
	if not duplicate_records_list: return
	x = 0
	for d in duplicate_records_list:
		if d.nos >1:
			print(d)
			frappe.db.sql('''
				DELETE FROM `tabRepost Item Valuation`
				WHERE name = %s
			''', d.name)

def repost_invoices_before(before_date="2021-02-10"):
	invoices = frappe.db.sql('''
	SELECT inv.name as name, inv.posting_date, inv.posting_time, set_warehouse, company
	FROM `tabSales Invoice` AS inv
	WHERE inv.name NOT IN (SELECT voucher_no FROM `tabRepost Item Valuation`)
	AND docstatus = 1
	AND posting_date <= %s
	ORDER BY posting_date ASC, posting_time ASC;
	''',before_date , as_dict=1)
	
	for inv in invoices:
		repost_inv = frappe.new_doc("Repost Item Valuation")
		repost_inv.voucher_type = "Sales Invoice"
		repost_inv.voucher_no = inv.name
		repost_inv.posting_date = inv.posting_date
		repost_inv.posting_time = inv.posting_time
		repost_inv.company = inv.company
		repost_inv.status = "Failed"
		repost_inv.save()
		repost_inv.submit()
		# print(repost_inv.name)

