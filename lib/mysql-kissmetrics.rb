require "mysql-kissmetrics/version"
require "mysql-kissmetrics/km"

module MysqlKissmetrics
    
    require 'dbi'
    require 'km'
    require 'date'

    def self.initialize(profile, username, password, km_key, allowed_history_days = 0)
        @allowed_history_days = allowed_history_days
        @now = DateTime.now.to_time.to_i
        @km_key = km_key
        conn = DBI.connect("dbi:ODBC:" << profile, username, password) do |dbh|
            threads = []
            ['purchases',
             'invoices',
             'rmas',
             'creditmemos',
             'tickets',
             'warranties',
             'outofstock',
             'canceledorders'].each{ |method|
              threads << Thread.new(dbh){|dbh| self.send('import_' << method, dbh)}
            }
            threads.each { |t| t.join }
        end
    end

    def self.import_outofstock(dbh)
      k = KM.new
      k.init(@km_key, :log_dir => 'log/')
      sth = dbh.execute("SELECT increment_id, DATE_FORMAT(DATE_ADD(MAX(created_at), INTERVAL -7 HOUR),'%b %d %Y %h:%i %p') AS created_at, customer_email, qty_ordered, SUM(qty_ordered) - SUM(qty_invoiced) AS items_not_fulfilled FROM (
                           SELECT b.increment_id, b.customer_email, b.status, b.created_at, IFNULL(a.qty_ordered,0) AS qty_ordered, IFNULL(a.qty_invoiced,0) AS qty_invoiced
                           FROM sales_flat_order AS b
                           INNER JOIN sales_flat_order_item AS a ON b.entity_id = a.order_id
                           WHERE b.created_at >= '2012-01-01' AND b.status != 'processing' AND b.status NOT LIKE '%canceled%' AND b.status NOT LIKE '%hold%' AND a.product_type = 'configurable' " <<
                           (@allowed_history_days > 0 ? " AND " << @now.to_s << " - UNIX_TIMESTAMP(DATE_ADD(b.created_at, INTERVAL -7 HOUR) ) <= " << (@allowed_history_days * 86000).to_s << " " : "") <<
                        ") AS x
                         WHERE qty_ordered <> qty_invoiced AND customer_email IS NOT NULL
                         GROUP BY customer_email
                         ORDER BY created_at DESC")
      while row = sth.fetch do
          next if row['customer_email'] == nil || row['customer_email'] == ''
          k.identify(row['customer_email'])
          ts = DateTime.parse(row['created_at']).to_time.to_i
          k.record('product is out-of-stock', {'Order ID' => row['increment_id'].to_i,
                                                'Type' => (row['qty_ordered'] == row['items_not_fulfilled'] ? 'Entire order' : 'Partial order'),
                                                '_d' => 1,
                                                '_t' => ts})
      end
      sth.finish
    end

    def self.import_warranties(dbh)
      k = KM.new
      k.init(@km_key, :log_dir => 'log/')
      sth = dbh.execute("SELECT DATE_FORMAT(DATE_ADD(a.date_added, INTERVAL -7 HOUR),'%b %d %Y %h:%i %p') AS created_at, a.customer_email, a.order_id, a.status, DATE_FORMAT(DATE_ADD(a.date_updated, INTERVAL -7 HOUR),'%b %d %Y %h:%i %p') AS date_updated FROM lotwarranty_warranty AS a WHERE customer_email LIKE '%@%' " <<
                         (@allowed_history_days > 0 ? " AND " << @now.to_s << " - UNIX_TIMESTAMP(DATE_ADD(a.date_added, INTERVAL -7 HOUR) ) <= " << (@allowed_history_days * 86000).to_s << " " : "") <<
                        "ORDER BY a.date_added DESC")
      while row = sth.fetch do
          next if row['customer_email'] == nil || row['customer_email'] == ''
          k.identify(row['customer_email'])
          ts = DateTime.parse(row['created_at']).to_time.to_i
          k.record('wants to warranty something', {'Order ID' => row['order_id'].to_i,
                                             '_d' => 1,
                                             '_t' => ts})
          if row['date_updated'] != nil && row['status'] != nil && row['status'].include?("resolved")
            ts = DateTime.parse(row['date_updated']).to_time.to_i
            k.record('warranty has been resolved', {'Order ID' => row['order_id'].to_i,
                                                     '_d' => 1,
                                                     '_t' => ts})
          end
      end
      sth.finish
    end

    def self.import_tickets(dbh)
      k = KM.new
      k.init(@km_key, :log_dir => 'log/')
      sth = dbh.execute("SELECT DATE_FORMAT(DATE_ADD(a.created_time, INTERVAL -7 HOUR),'%b %d %Y %h:%i %p') AS created_at, CASE WHEN a.title OR b.name LIKE '%phone%' THEN 'phoned us' WHEN a.title LIKE 'Chat transcript%' THEN 'chatted with us' ELSE 'emailed us' END AS channel, a.customer_email, a.order_id AS increment_id, b.name AS reason
                         FROM aw_hdu_ticket AS a
                         INNER JOIN aw_hdu_department AS b ON a.department_id = b.id
                         WHERE customer_email != 'anonymous@gmail.com' AND customer_email != 'anon' AND customer_email NOT LIKE 'none@%' " << 
                         (@allowed_history_days > 0 ? " AND " << @now.to_s << " - UNIX_TIMESTAMP(DATE_ADD(a.created_time, INTERVAL -7 HOUR) ) <= " << (@allowed_history_days * 86000).to_s << " " : "") <<
                        "ORDER BY a.created_time DESC")
      while row = sth.fetch do
          next if row['customer_email'] == nil || row['customer_email'] == ''
          k.identify(row['customer_email'])
          ts = DateTime.parse(row['created_at']).to_time.to_i
          k.record(row['channel'], {'Order ID' => row['increment_id'].to_i,
                                     'Reason for Ticket' => row['reason'],
                                     '_d' => 1,
                                     '_t' => ts})
      end
      sth.finish
    end

    def self.import_rmas(dbh)
      k = KM.new
      k.init(@km_key, :log_dir => 'log/')
      sth = dbh.execute("SELECT a.increment_id, a.customer_email, DATE_FORMAT(DATE_ADD(b.created_at, INTERVAL -7 HOUR),'%b %d %Y %h:%i %p') AS created_at, c.name AS request_type
                         FROM sales_flat_order AS a
                         INNER JOIN aw_rma_entity AS b ON a.entity_id = b.order_id
                         INNER JOIN aw_rma_entity_types AS c ON b.request_type = c.id " <<
                         (@allowed_history_days > 0 ? "WHERE  " << @now.to_s << " - UNIX_TIMESTAMP(DATE_ADD(b.created_at, INTERVAL -7 HOUR) ) <= " << (@allowed_history_days * 86000).to_s << " " : "") <<
                        "ORDER BY b.created_at DESC")
      while row = sth.fetch do
          next if row['customer_email'] == nil || row['customer_email'] == ''
          k.identify(row['customer_email'])
          ts = DateTime.parse(row['created_at']).to_time.to_i
          k.record('wants to return something', {'Order ID' => row['increment_id'].to_i,
                                                                    'Reason for Return' => row['request_type'],
                                                                    '_d' => 1,
                                                                    '_t' => ts})
      end
      sth.finish
    end

    def self.import_canceledorders(dbh)
      k = KM.new
      k.init(@km_key, :log_dir => 'log/')
      sth = dbh.execute("SELECT b.label AS status, a.increment_id, a.customer_email, DATE_FORMAT(DATE_ADD(a.updated_at, INTERVAL -7 HOUR),'%b %d %Y %h:%i %p') AS updated_at, 0 - a.grand_total AS grand_total, 0 - a.subtotal AS subtotal
                         FROM sales_flat_order AS a
                         INNER JOIN sales_order_status AS b ON a.status = b.status
                         WHERE a.status LIKE 'cancel%' " <<
                         (@allowed_history_days > 0 ? " AND " << @now.to_s << " - UNIX_TIMESTAMP(DATE_ADD(a.updated_at, INTERVAL -7 HOUR) ) <= " << (@allowed_history_days * 86000).to_s << " " : "") <<
                        "ORDER BY a.updated_at DESC")
      while row = sth.fetch do
          next if row['customer_email'] == nil || row['customer_email'] == ''
          k.identify(row['customer_email'])
          ts = DateTime.parse(row['updated_at']).to_time.to_i
          k.record('order has been canceled', {'Order ID' => row['increment_id'].to_i,
                                                'Cancel Total' => row['grand_total'].to_f,
                                                'Reason' => row['status'],
                                                '_d' => 1,
                                                '_t' => ts})
      end
      sth.finish      
    end    

    def self.import_invoices(dbh)
      k = KM.new
      k.init(@km_key, :log_dir => 'log/')
      sth = dbh.execute("SELECT a.increment_id, a.customer_email, DATE_FORMAT(DATE_ADD(b.created_at, INTERVAL -7 HOUR),'%b %d %Y %h:%i %p') AS created_at, b.grand_total, b.subtotal
                         FROM sales_flat_order AS a
                         INNER JOIN sales_flat_invoice AS b ON a.entity_id = b.order_id " <<
                         (@allowed_history_days > 0 ? "WHERE  " << @now.to_s << " - UNIX_TIMESTAMP(DATE_ADD(b.created_at, INTERVAL -7 HOUR) ) <= " << (@allowed_history_days * 86000).to_s << " " : "") <<
                        "ORDER BY b.created_at DESC")
      while row = sth.fetch do
          next if row['customer_email'] == nil || row['customer_email'] == ''
          k.identify(row['customer_email'])
          ts = DateTime.parse(row['created_at']).to_time.to_i
          k.record('order has been shipped', {'Order ID' => row['increment_id'].to_i,
                                               'Invoice Total' => row['grand_total'].to_f,
                                               '_d' => 1,
                                               '_t' => ts})
      end
      sth.finish      
    end

    def self.import_creditmemos(dbh)
      k = KM.new
      k.init(@km_key, :log_dir => 'log/')
      sth = dbh.execute("SELECT a.increment_id, a.customer_email, DATE_FORMAT(DATE_ADD(b.created_at, INTERVAL -7 HOUR),'%b %d %Y %h:%i %p') AS created_at, 0 - b.grand_total AS grand_total, 0 - b.subtotal AS subtotal
                         FROM sales_flat_order AS a
                         INNER JOIN sales_flat_creditmemo AS b ON a.entity_id = b.order_id " <<
                         (@allowed_history_days > 0 ? "WHERE  " << @now.to_s << " - UNIX_TIMESTAMP(DATE_ADD(b.created_at, INTERVAL -7 HOUR) ) <= " << (@allowed_history_days * 86000).to_s << " " : "") <<
                        "ORDER BY b.created_at DESC")
      while row = sth.fetch do
          next if row['customer_email'] == nil || row['customer_email'] == ''
          k.identify(row['customer_email'])
          ts = DateTime.parse(row['created_at']).to_time.to_i
          k.record('order has been refunded', {'Order ID' => row['increment_id'].to_i,
                                       'Refund Total' => row['grand_total'].to_f,
                                       '_d' => 1,
                                       '_t' => ts})
      end
      sth.finish  
    end

    def self.import_purchases(dbh)
      k = KM.new
      k.init(@km_key, :log_dir => 'log/')
      sth = dbh.execute("SELECT a.entity_id, DATE_FORMAT(DATE_ADD(a.created_at, INTERVAL -7 HOUR),'%b %d %Y %h:%i %p') AS created_at, a.customer_email, a.customer_firstname, a.customer_lastname, a.increment_id, a.grand_total, a.subtotal, b.region, b.postcode, b.city, a.coupon_code
                         FROM sales_flat_order AS a
                         INNER JOIN sales_flat_order_address AS b ON a.shipping_address_id = b.entity_id AND b.address_type = 'shipping' " <<
                         (@allowed_history_days > 0 ? "WHERE  " << @now.to_s << " - UNIX_TIMESTAMP(DATE_ADD(a.created_at, INTERVAL -7 HOUR) ) <= " << (@allowed_history_days * 86000).to_s << " " : "") <<
                        "ORDER BY increment_id DESC")
      while row = sth.fetch do
          next if row['customer_email'] == nil || row['customer_email'] == ''
          k.identify(row['customer_email'])

          k.set({"First Name" => row['customer_firstname'],
                  "Last Name" => row['customer_lastname'],
                  "Province" => row['region'], 
                  "Postal Code" => row['postcode'],
                  "City" => row['city']})

          ts = DateTime.parse(row['created_at']).to_time.to_i
          k.record('bought something', {'Order ID' => row['increment_id'].to_i,
                                         'Order Total' => row['grand_total'].to_f,
                                         'Coupon Code' => row['coupon_code'],
                                         '_d' => 1,
                                         '_t' => ts})

          xth = dbh.execute("SELECT x.*, CASE WHEN deal_qty > 0 THEN 'Deal of the Day' WHEN msrp = price THEN 'Full Price' WHEN price < msrp THEN CONCAT('On-Sale ', FORMAT((1-(price/msrp))*100,0), '%') END AS type FROM (
                               SELECT b.qty_ordered, t.value AS category, p.qty_ordered AS deal_qty, q.value AS msrp, b.sku, k.value AS brand, CASE WHEN r.value LIKE '%,%' THEN 'Unisex' ELSE s.value END AS department, d.value AS style, j.value AS season, i.value AS product, n.value AS color, o.value AS size, (SELECT MAX(price) FROM sales_flat_order_item WHERE order_id = b.order_id AND sku = b.sku) AS price
                               FROM sales_flat_order AS a
                               INNER JOIN sales_flat_order_item AS b ON a.entity_id = b.order_id AND product_type = 'simple'
                               LEFT JOIN catalog_product_entity_int AS c ON b.product_id = c.entity_id AND c.attribute_id = (SELECT attribute_id FROM eav_attribute WHERE attribute_code = 'manufacturer' AND entity_type_id = 4)
                               LEFT JOIN catalog_product_entity_varchar AS d ON b.product_id = d.entity_id AND d.attribute_id = (SELECT attribute_id FROM eav_attribute WHERE attribute_code = 'vendor_product_id' AND entity_type_id = 4)
                               LEFT JOIN catalog_product_entity_int AS f ON b.product_id = f.entity_id AND f.attribute_id = (SELECT attribute_id FROM eav_attribute WHERE attribute_code = 'season_id' AND entity_type_id = 4)
                               LEFT JOIN catalog_product_entity_varchar AS i ON b.product_id = i.entity_id AND i.attribute_id = (SELECT attribute_id FROM eav_attribute WHERE attribute_code = 'name' AND entity_type_id = 4)
                               LEFT JOIN eav_attribute_option_value AS j ON f.value = j.option_id AND j.store_id = 0
                               LEFT JOIN eav_attribute_option_value AS k ON c.value = k.option_id AND k.store_id = 0
                               LEFT JOIN catalog_product_entity_int AS l ON b.product_id = l.entity_id AND l.attribute_id = (SELECT attribute_id FROM eav_attribute WHERE attribute_code = 'choose_color' AND entity_type_id = 4)
                               LEFT JOIN catalog_product_entity_int AS m ON b.product_id = m.entity_id AND m.attribute_id = (SELECT attribute_id FROM eav_attribute WHERE attribute_code = 'choose_size' AND entity_type_id = 4)
                               LEFT JOIN eav_attribute_option_value AS n ON l.value = n.option_id AND n.store_id = 0
                               LEFT JOIN eav_attribute_option_value AS o ON m.value = o.option_id AND o.store_id = 0
                               LEFT JOIN aw_collpur_deal_purchases AS p ON b.order_id = p.order_id AND p.order_item_id = (SELECT item_id FROM sales_flat_order_item WHERE order_id = b.order_id AND sku = b.sku ORDER BY price DESC LIMIT 0,1)
                               LEFT JOIN catalog_product_entity_decimal AS q ON b.product_id = q.entity_id AND q.attribute_id = (SELECT attribute_id FROM eav_attribute WHERE attribute_code = 'price' AND entity_type_id = 4)
                               LEFT JOIN catalog_product_entity_varchar AS r ON b.product_id = r.entity_id AND r.attribute_id = (SELECT attribute_id FROM eav_attribute WHERE attribute_code = 'department' AND entity_type_id = 4)
                               LEFT JOIN eav_attribute_option_value AS s ON r.value = s.option_id AND s.store_id = 0
                               LEFT JOIN catalog_product_entity_varchar AS t ON (SELECT parent_id FROM catalog_product_super_link WHERE product_id = b.product_id LIMIT 0,1) = t.entity_id AND t.attribute_id = (SELECT attribute_id FROM eav_attribute WHERE attribute_code = 'category_text' AND entity_type_id = 4)                             
                               WHERE a.entity_id = " << row['entity_id'].to_s << ") AS x")
          while item = xth.fetch do
              ts = ts + 1

              if item['brand'] != "" && item['brand'] != nil
                  k.set({"Brand" => item['brand'],
                          "Product" => item['product'],
                          "Color" => item['color'],
                          "Size" => item['size'],
                          "Price" => item['price'].to_f,
                          "Merchandise" => item['type'],
                          "Department" => item['department'],
                          "Category" => item['category'],
                          "Qty" => item['qty_ordered'].to_i,
                          "_t" => ts,
                          "_d" => 1})
              end
          end
          xth.finish
      end
      sth.finish      
    end
end