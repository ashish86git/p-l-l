from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from decimal import Decimal
import pandas as pd
import numpy as np
import re

from flask_sqlalchemy import SQLAlchemy
from decimal import Decimal
import datetime

app = Flask(__name__)
app.secret_key = "secret123"

# ------------------------
# GLOBAL DATA (Mock DB)

# ✅ PostgreSQL Connection via SQLAlchemy
app.config['SQLALCHEMY_DATABASE_URI'] = (
    'postgresql://{user}:{password}@{host}:{port}/{database}'.format(
        user='u7tqojjihbpn7s',
        password='p1b1897f6356bab4e52b727ee100290a84e4bf71d02e064e90c2c705bfd26f4a5',
        host='c7s7ncbk19n97r.cluster-czrs8kj4isg7.us-east-1.rds.amazonaws.com',
        port=5432,
        database='d8lp4hr6fmvb9m'
    )
)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Initialize DB
db = SQLAlchemy(app)

# ---------------------------------------------
# DATABASE MODEL
# ---------------------------------------------
class DailyInputData(db.Model):
    __tablename__ = "daily_input_data"

    id = db.Column(db.Integer, primary_key=True)
    input_date = db.Column(db.Date, nullable=False)
    customer_key = db.Column(db.String(50), nullable=False)
    location_key = db.Column(db.String(50), nullable=False)
    field_name = db.Column(db.String(100), nullable=False)
    field_value = db.Column(db.Numeric(15, 2), nullable=False)

    __table_args__ = (
        db.UniqueConstraint('input_date', 'customer_key', 'location_key', 'field_name'),
    )

    def __repr__(self):
        return f"<DailyInputData {self.customer_key} | {self.location_key} | {self.input_date}>"




# ------------------------


class MasterManpower(db.Model):
    __tablename__ = "master_manpower"
    id = db.Column(db.Integer, primary_key=True)
    customer = db.Column(db.String(255), nullable=False)
    role_name = db.Column(db.String(255), nullable=False)
    location = db.Column(db.String(255), nullable=False)
    monthly_salary = db.Column(db.Float, default=0)
    daily_cost = db.Column(db.Float, default=0)
    ot_cost = db.Column(db.Float, default=0)


class MasterOperational(db.Model):
    __tablename__ = "master_operational"
    id = db.Column(db.Integer, primary_key=True)
    customer = db.Column(db.String(255), nullable=False)
    cost_type = db.Column(db.String(255), nullable=False)
    location = db.Column(db.String(255), nullable=False)
    daily_cost = db.Column(db.Float, default=0)
    type_ = db.Column("type", db.String(100), nullable=True)


class MasterConsumables(db.Model):
    __tablename__ = "master_consumables"
    id = db.Column(db.Integer, primary_key=True)
    customer = db.Column(db.String(255), nullable=False)
    item_name = db.Column(db.String(255), nullable=False)
    location = db.Column(db.String(255), nullable=False)
    unit_cost = db.Column(db.Float, default=0)
    quantity = db.Column(db.Integer, default=0)

with app.app_context():
    db.create_all()
# -----------------------------
# LOCATIONS & CUSTOMERS (for demo)
# -----------------------------
LOCATIONS = ["Hyderabad", "Gurgaon"]
CUSTOMERS = ["kothari_kickers","lifelong","hike","eshopbox","spario"]

DAILY_INPUTS = []

# ------------------------
# ROUTES
# ------------------------


def get_filter_options_db():
    """Fetches unique customers and locations from DailyInputData for filters."""
    customers = db.session.query(distinct(DailyInputData.customer_key)).all()
    locations = db.session.query(distinct(DailyInputData.location_key)).all()

    # Flatten the list of tuples for Jinja iteration
    return {
        'customers': sorted([c[0] for c in customers]),
        'locations': sorted([l[0] for l in locations])
    }


def fetch_master_rates():
    """Fetches all master rates into fast-lookup dictionaries."""

    # 1. Manpower Rates: Key = (customer, location, role_name) -> daily_cost
    manpower_rates = {
        (m.customer, m.location, m.role_name): Decimal(m.daily_cost)
        for m in MasterManpower.query.all()
    }

    # 2. Operational Rates: Key = (customer, location, cost_type) -> daily_cost
    operational_rates = {
        (o.customer, o.location, o.cost_type): Decimal(o.daily_cost)
        for o in MasterOperational.query.all()
    }

    # 3. Consumables Rates: Key = (customer, location, item_name) -> unit_cost
    consumables_rates = {
        (c.customer, c.location, c.item_name): Decimal(c.unit_cost)
        for c in MasterConsumables.query.all()
    }

    return manpower_rates, operational_rates, consumables_rates


def calculate_pl_summary_db(date_filter=None, customer_filter=None, location_filter=None):
    """
    Core function to calculate P&L summary based on filtered DailyInputData.
    This uses pre-fetched master rates to avoid N+1 queries.
    """

    # 1. Fetch all master rates once
    manpower_rates, operational_rates, consumables_rates = fetch_master_rates()

    # 2. Build the query for Daily Input Data based on filters
    query = DailyInputData.query

    conditions = []
    if date_filter:
        try:
            date_obj = datetime.datetime.strptime(date_filter, "%Y-%m-%d").date()
            conditions.append(DailyInputData.input_date == date_obj)
        except ValueError:
            pass  # Ignore invalid date

    if customer_filter:
        conditions.append(DailyInputData.customer_key == customer_filter)

    if location_filter:
        conditions.append(DailyInputData.location_key == location_filter)

    if conditions:
        query = query.filter(and_(*conditions))

    daily_inputs = query.all()

    # 3. Process the results (Grouping and Calculation)
    summary = {}

    for inp in daily_inputs:
        key = (inp.input_date.isoformat(), inp.customer_key, inp.location_key)

        if key not in summary:
            summary[key] = {
                'date': inp.input_date.isoformat(),
                'customer': inp.customer_key,
                'location': inp.location_key,
                'revenue': Decimal(0),
                'manpower_cost': Decimal(0),
                'operational_cost': Decimal(0),
                'consumables_cost': Decimal(0),
            }

        input_value = inp.field_value
        customer = inp.customer_key
        location = inp.location_key
        field_name = inp.field_name

        rate_key = (customer, location, field_name)

        if field_name.startswith("employee_"):
            rate = manpower_rates.get(rate_key)
            if rate is not None:
                summary[key]['manpower_cost'] += rate * input_value

        elif field_name.startswith("op_"):
            rate = operational_rates.get(rate_key)
            if rate is not None:
                summary[key]['operational_cost'] += rate * input_value

        elif field_name.startswith("cons_"):
            rate = consumables_rates.get(rate_key)
            if rate is not None:
                summary[key]['consumables_cost'] += rate * input_value

        elif field_name.startswith("revenue_"):
            summary[key]['revenue'] += input_value

    # 4. Finalize calculations and formatting
    results = list(summary.values())
    for res in results:
        total_cost = res['manpower_cost'] + res['operational_cost'] + res['consumables_cost']
        profit = res['revenue'] - total_cost

        res['total_cost'] = total_cost
        res['profit'] = profit

        # Formatting for display (important for the AJAX response)
        res['revenue_display'] = f'₹{res["revenue"]:,.0f}'
        res['total_cost_display'] = f'₹{total_cost:,.0f}'
        res['profit_display'] = f'₹{profit:,.0f}'
        res['manpower_cost_display'] = f'₹{res["manpower_cost"]:,.0f}'
        res['operational_cost_display'] = f'₹{res["operational_cost"]:,.0f}'
        res['consumables_cost_display'] = f'₹{res["consumables_cost"]:,.0f}'
        res['profit_class'] = 'profit-positive' if profit >= 0 else 'profit-negative'

    return results


@app.route('/')
def index():
    return render_template('index.html')


@app.route("/master", methods=["GET", "POST"])
def master():
    if request.method == "POST":
        category = request.form.get("category")
        customer = request.form.get("customer")
        location = request.form.get("location")
        try:
            if category == "manpower":
                role_name = request.form.get("role_name")
                monthly_salary = float(request.form.get("monthly_salary") or 0)
                ot_cost = float(request.form.get("ot_cost") or 0)

                if category == "manpower":
                    role_name = request.form.get("role_name")
                    monthly_salary = float(request.form.get("monthly_salary") or 0)
                    ot_cost = float(request.form.get("ot_cost") or 0)

                    daily_cost = round(monthly_salary / (30 if "security_guard" in role_name.lower() else 26), 2)

                entry = MasterManpower(
                    customer=customer,
                    location=location,
                    role_name=role_name,
                    monthly_salary=monthly_salary,
                    daily_cost=daily_cost,
                    ot_cost=ot_cost
                )
                db.session.add(entry)
                flash(f"Manpower '{role_name}' added successfully!", "success")


            elif category == "operational":

                cost_type = request.form.get("cost_type")

                daily_cost = float(request.form.get("daily_cost") or 0)

                type_value = request.form.get("type")  # dropdown se aata hai

                entry = MasterOperational(

                    customer=customer,

                    location=location,

                    cost_type=cost_type,

                    daily_cost=daily_cost,

                    type_=type_value  # 👈 alias use karo

                )

                db.session.add(entry)

                flash(f"Operational cost '{cost_type}' added successfully!", "success")

            elif category == "consumables":
                item_name = request.form.get("item_name")
                unit_cost = float(request.form.get("unit_cost") or 0)
                quantity = int(request.form.get("quantity") or 0)

                entry = MasterConsumables(
                    customer=customer,
                    location=location,
                    item_name=item_name,
                    unit_cost=unit_cost,
                    quantity=quantity
                )
                db.session.add(entry)
                flash(f"Consumable '{item_name}' added successfully!", "success")

            db.session.commit()

        except Exception as e:
            db.session.rollback()
            flash(f"Error: {str(e)}", "danger")

        return redirect(url_for("master"))

    # --- GET request: fetch existing data ---
    manpower_data = MasterManpower.query.all()
    operational_data = MasterOperational.query.all()
    consumables_data = MasterConsumables.query.all()

    # --- Build customer list from all master tables ---
    all_customers = set()
    for t in [manpower_data, operational_data, consumables_data]:
        all_customers.update([r.customer for r in t if r.customer])
    customers = sorted(list(all_customers))

    # --- Build customer -> location mapping ---
    customer_locations = {}
    for cust in customers:
        locs = set()
        for r in manpower_data + operational_data + consumables_data:
            if r.customer == cust:
                locs.add(r.location)
        customer_locations[cust] = sorted(list(locs))

    return render_template(
        "master.html",
        customers=customers,
        customer_locations=customer_locations,
        manpower_data=manpower_data,
        operational_data=operational_data,
        consumables_data=consumables_data
    )



@app.route("/delete/<category>/<int:index>")
def delete_entry(category, index):
    try:
        if category == "manpower":
            entry = MasterManpower.query.get_or_404(index)
        elif category == "operational":
            entry = MasterOperational.query.get_or_404(index)
        elif category == "consumables":
            entry = MasterConsumables.query.get_or_404(index)
        else:
            flash("Invalid category!", "danger")
            return redirect(url_for("master"))

        db.session.delete(entry)
        db.session.commit()
        flash(f"Deleted {category} entry successfully.", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Error deleting entry: {str(e)}", "danger")

    return redirect(url_for("master"))



@app.route("/daily_input", methods=["GET", "POST"])
def daily_input():
    today = datetime.date.today().isoformat()

    # --- Fetch customers and locations from master tables ---
    manpower_data = MasterManpower.query.all()
    operational_data = MasterOperational.query.all()
    consumables_data = MasterConsumables.query.all()

    # Build customer list
    customers_set = set()
    for t in [manpower_data, operational_data, consumables_data]:
        customers_set.update([r.customer for r in t if r.customer])
    customers = sorted(list(customers_set))

    # Build customer -> location mapping
    customer_locations = {}
    for cust in customers:
        locs = set()
        for r in manpower_data + operational_data + consumables_data:
            if r.customer == cust:
                locs.add(r.location)
        customer_locations[cust] = sorted(list(locs))

    if request.method == "POST":
        form_data = request.form.to_dict()
        date = form_data.get("date")
        customer = form_data.get("customer")
        location = form_data.get("location")

        # Basic Validation
        if not date or not customer or not location:
            flash("Please select Date, Customer, and Location!", "danger")
            return redirect(url_for("daily_input"))

        if customer not in customers:
            flash("Invalid customer selected!", "danger")
            return redirect(url_for("daily_input"))

        if location not in customer_locations.get(customer, []):
            flash("Invalid location for selected customer!", "danger")
            return redirect(url_for("daily_input"))

        try:
            input_date = datetime.datetime.strptime(date, "%Y-%m-%d").date()

            # Loop through all form fields
            for field_name, field_value in form_data.items():
                if field_name in ["date", "customer", "location"]:
                    continue  # skip meta fields

                try:
                    val = Decimal(field_value.strip() or 0)
                except:
                    val = Decimal(0)

                # Check if record already exists (update)
                record = DailyInputData.query.filter_by(
                    input_date=input_date,
                    customer_key=customer,
                    location_key=location,
                    field_name=field_name
                ).first()

                if record:
                    record.field_value = val
                else:
                    new_entry = DailyInputData(
                        input_date=input_date,
                        customer_key=customer,
                        location_key=location,
                        field_name=field_name,
                        field_value=val
                    )
                    db.session.add(new_entry)

            db.session.commit()
            flash(f"✅ Data saved successfully for {customer} - {location} ({date})", "success")

        except Exception as e:
            db.session.rollback()
            flash(f"❌ Database error: {e}", "danger")

        return redirect(url_for("index"))

    # --- GET request: render template with DB-driven dropdowns ---
    return render_template(
        "input.html",
        today=today,
        customers=customers,
        customer_locations=customer_locations
    )


@app.route("/summary")
def summary():
    import re
    from decimal import Decimal

    def normalize_key(s):
        """Normalize key for consistent comparison."""
        return re.sub(r'[^a-z0-9]', '', (s or "").lower().strip())

    # Field display mapping
    FIELD_MAPPING = {
        "house_keeping": "House Keeping",
        "security_guard": "Security Guard",
        "security_guard_female": "Security Guard Female",
        "security_supervisor": "Security Supervisor",
        "blue_collar": "Blue Collar",
        "loading_unloading": "Loading & Unloading",
        "electrician": "Electrician",
        "adhoc_manpower": "Adhoc Manpower",
        "supervisor_team_lead": "Supervisor Team Lead",
        "supervisor_ex_off_roll": "Supervisor Ex Off Roll",
        "supervisor_ex_off_roll_deo": "Supervisor Ex Off Roll DEO",
        "outbound_cbm": "Outbound/CBM",
        "storage_day_cbm": "Storage/Day/CBM",
        "white_collar": "White Collar",
        "overtime_supervisor": "Overtime Supervisor",
        "overtime_blue_collar": "Overtime Blue Collar",
        "overtime_blue_collar_loading_unloading_blue_collar": "Overtime Blue Collar Loading Unloading Blue Collar",
    }

    # Fields that are always part of "Other Cost"
    OTHER_COST_FIELDS = [
        "tea", "water", "internet", "wms", "stationery", "electricity",
        "electricity_sub_meter", "diesel", "staff_welfare", "convence",
        "ho_cost", "traveling_cost", "hra", "capex", "hk_materials",
        "other_expenses", "rr_cost", "rental", "pen_pencil", "cartridge", "bubble_wrap", "stretch_wrap"
        "roll_100x150", "roll_75x50", "roll_25x50", "a4_paper", "ribbon_25x50"
    ]
    OTHER_COST_FIELDS_NORM = [normalize_key(f) for f in OTHER_COST_FIELDS]

    # Fields that should directly add into Manpower (not rate-based)
    DIRECT_MANPOWER_FIELDS = [
        "white_collar", "supervisor_team_lead",
        "supervisor_ex_off_roll", "supervisor_ex_off_roll_deo","supervisor_deo"
    ]
    DIRECT_MANPOWER_NORM = [normalize_key(f) for f in DIRECT_MANPOWER_FIELDS]

    # ------------------ Filters ------------------
    start_date = request.args.get('start_date', '').strip()
    end_date = request.args.get('end_date', '').strip()
    customer_filter = normalize_key(request.args.get('customer', ''))
    location_filter = normalize_key(request.args.get('location', ''))

    # Get all master customer-location pairs
    master_groups = db.session.query(
        MasterOperational.customer, MasterOperational.location
    ).distinct().all()

    # Get all available dates
    all_dates = [r[0] for r in db.session.query(DailyInputData.input_date).distinct().all()]

    summary_data, breakdown_data = [], []

    # ------------------ MAIN LOOP ------------------
    for date in all_dates:
        for mg in master_groups:
            customer = (mg.customer or "").strip()
            location = (mg.location or "").strip()
            cust_norm, loc_norm = normalize_key(customer), normalize_key(location)

            # Filter by date and customer/location
            if start_date and str(date) < start_date:
                continue
            if end_date and str(date) > end_date:
                continue
            if customer_filter and customer_filter not in cust_norm:
                continue
            if location_filter and location_filter not in loc_norm:
                continue

            # Load daily inputs
            inputs = DailyInputData.query.filter_by(
                input_date=date, customer_key=customer, location_key=location
            ).all()
            if not inputs:
                continue

            # Convert to dict {field: value}
            input_dict = {
                normalize_key(FIELD_MAPPING.get(normalize_key(i.field_name), i.field_name)): Decimal(str(i.field_value or 0))
                for i in inputs
            }

            # Load master data
            manpower_master = MasterManpower.query.filter_by(customer=customer, location=location).all()
            operational_master = MasterOperational.query.filter_by(customer=customer, location=location).all()

            # Build rate dictionaries
            man_rate, op_cost, op_rate = {}, {}, {}
            for m in manpower_master:
                key = normalize_key(FIELD_MAPPING.get(normalize_key(m.role_name), m.role_name))
                man_rate[key] = Decimal(str(m.daily_cost or 0))
            for o in operational_master:
                key = normalize_key(FIELD_MAPPING.get(normalize_key(o.cost_type), o.cost_type))
                if (o.type_ or "").lower() == "cost":
                    op_cost[key] = Decimal(str(o.daily_cost or 0))
                elif (o.type_ or "").lower() == "revenue":
                    op_rate[key] = Decimal(str(o.daily_cost or 0))

            # ------------------ MANPOWER COST ------------------
            manpower_cost = Decimal("0.0")

            # Add rate-based manpower cost
            for role, rate in man_rate.items():
                qty = input_dict.get(role, 0)
                if qty:
                    amt = qty * rate
                    manpower_cost += amt
                    breakdown_data.append({
                        "date": date, "customer": customer, "location": location,
                        "category": "Manpower", "field": FIELD_MAPPING.get(role, role),
                        "quantity": float(qty), "rate": float(rate), "amount": float(amt)
                    })

            # Add direct manpower (input amount directly)
            for field in DIRECT_MANPOWER_NORM:
                val = input_dict.get(field, 0)
                if val:
                    manpower_cost += val
                    breakdown_data.append({
                        "date": date, "customer": customer, "location": location,
                        "category": "Manpower",
                        "field": FIELD_MAPPING.get(field, field),
                        "quantity": float(val), "rate": 0.0, "amount": float(val)
                    })

            # ------------------ OTHER COST ------------------
            other_cost = Decimal("0.0")
            for key, val in input_dict.items():
                if normalize_key(key) in OTHER_COST_FIELDS_NORM and val:
                    other_cost += val
                    breakdown_data.append({
                        "date": date, "customer": customer, "location": location,
                        "category": "Other Cost", "field": FIELD_MAPPING.get(key, key),
                        "quantity": float(val), "rate": 0.0, "amount": float(val)
                    })

            # ------------------ REVENUE ------------------
            revenue = Decimal("0.0")

            # Lifelong special case
            if "lifelong" in cust_norm:
                # Normal revenue calculation
                for field, rate in op_rate.items():
                    qty = input_dict.get(field, 0)
                    if qty:
                        amt = qty * rate
                        revenue += amt
                        breakdown_data.append({
                            "date": date, "customer": customer, "location": location,
                            "category": "Revenue",
                            "field": FIELD_MAPPING.get(field, field),
                            "quantity": float(qty), "rate": float(rate), "amount": float(amt)
                        })

                # Add Tea + Staff Welfare also in revenue
                for special in ["tea", "staff_welfare","roll_100x150","roll_75x50","roll_25x50","a4_paper","pen_pencil","cartridge","bubble_wrap","stretch_wrap"]:
                    val = input_dict.get(normalize_key(special), 0)
                    if val:
                        revenue += val
                        breakdown_data.append({
                            "date": date, "customer": customer, "location": location,
                            "category": "Revenue",
                            "field": FIELD_MAPPING.get(special, special),
                            "quantity": float(val), "rate": 0.0, "amount": float(val)
                        })

            # Other customers - normal revenue
            else:
                for field, rate in op_rate.items():
                    qty = input_dict.get(field, 0)
                    if qty:
                        amt = qty * rate
                        revenue += amt
                        breakdown_data.append({
                            "date": date, "customer": customer, "location": location,
                            "category": "Revenue",
                            "field": FIELD_MAPPING.get(field, field),
                            "quantity": float(qty), "rate": float(rate), "amount": float(amt)
                        })

            # ------------------ FINAL CALCULATIONS ------------------
            total_cost = manpower_cost + other_cost
            gross_profit = revenue - manpower_cost
            net_profit = revenue - total_cost
            margin = (net_profit / revenue * 100) if revenue > 0 else 0

            summary_data.append({
                "date": date, "customer": customer, "location": location,
                "revenue": float(round(revenue, 2)),
                "manpower_cost": float(round(manpower_cost, 2)),
                "other_cost": float(round(other_cost, 2)),
                "total_cost": float(round(total_cost, 2)),
                "gross_profit": float(round(gross_profit, 2)),
                "net_profit": float(round(net_profit, 2)),
                "net_profit_margin": float(round(margin, 2))
            })

    # ------------------ CATEGORY SUMMARY ------------------
    category_summary = {}
    for item in breakdown_data:
        key = (item['date'], item['customer'], item['location'], item['field'])
        if key not in category_summary:
            category_summary[key] = {
                "date": item['date'], "customer": item['customer'], "location": item['location'],
                "attributes": item['field'], "cost": 0.0, "revenue": 0.0
            }
        if item['category'] in ["Manpower", "Other Cost"]:
            category_summary[key]["cost"] += item['amount']
        elif item['category'] == "Revenue":
            category_summary[key]["revenue"] += item['amount']

    category_breakdown_summary = [
        {**v, "cost": round(v["cost"], 2), "revenue": round(v["revenue"], 2)}
        for v in category_summary.values()
    ]

    # ------------------ TOTALS ------------------
    total_revenue = sum(i["revenue"] for i in summary_data)
    total_cost = sum(i["total_cost"] for i in summary_data)
    total_profit = sum(i["net_profit"] for i in summary_data)
    avg_margin = (total_profit / total_revenue * 100) if total_revenue else 0

    # ------------------ RENDER ------------------
    return render_template(
        "summary.html",
        summary_data=sorted(summary_data, key=lambda x: x['date'], reverse=True),
        breakdown_data=breakdown_data,
        category_breakdown_summary=sorted(category_breakdown_summary, key=lambda x: x['date'], reverse=True),
        total_revenue=round(total_revenue, 2),
        total_cost=round(total_cost, 2),
        total_profit=round(total_profit, 2),
        avg_margin=round(avg_margin, 2)
    )




@app.route("/config")
def config():
    return render_template("config.html")


# ------------------------
# RUN APP
# ------------------------
if __name__ == "__main__":
    app.run(debug=True)
