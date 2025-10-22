from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from decimal import Decimal
import pandas as pd
import numpy as np
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
                daily_cost = round(monthly_salary / 30, 2)

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

                entry = MasterOperational(
                    customer=customer,
                    location=location,
                    cost_type=cost_type,
                    daily_cost=daily_cost
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





# ✅ FIXED DAILY INPUT ROUTE
# ✅ DAILY INPUT ROUTE — Save data to DB
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


@app.route('/summary')
def summary():
    # Fetch distinct groups
    groups = db.session.query(
        DailyInputData.input_date,
        DailyInputData.customer_key,
        DailyInputData.location_key
    ).distinct().all()

    summary_data = []

    for g in groups:
        date = g.input_date
        customer = g.customer_key
        location = g.location_key

        # Fetch all input rows for this group
        inputs = DailyInputData.query.filter_by(
            input_date=date,
            customer_key=customer,
            location_key=location
        ).all()

        # Convert to dict for easy lookup
        input_dict = {row.field_name.lower(): float(row.field_value or 0) for row in inputs}

        # Fetch master rates
        manpower = MasterManpower.query.filter_by(customer=customer, location=location).all()
        operational = MasterOperational.query.filter_by(customer=customer, location=location).all()

        man_rate = {m.role_name.lower(): float(m.daily_cost or 0) for m in manpower}
        man_ot = {m.role_name.lower(): float(m.ot_cost or 0) for m in manpower}
        op_rate = {o.cost_type.lower(): float(o.daily_cost or 0) for o in operational}

        # --- Formula Application ---

        # Variable Revenue
        outbound_cbm = input_dict.get("outbound_cbm", 0)
        tea = input_dict.get("tea", 0)
        staff_welfare = input_dict.get("staff_welfare", 0)
        outbound_rate = op_rate.get("outbound/cbm", 0)

        variable_revenue = (outbound_cbm * outbound_rate) + tea + staff_welfare

        # Total Revenue
        storage_day_cbm = input_dict.get("storage_day_cbm", 0)
        storage_rate = op_rate.get("storage/day/cbm", 0)
        total_revenue = (storage_day_cbm * storage_rate) + variable_revenue

        # Calculate Total Staff Cost
        total_staff_cost = 0
        total_staff_cost += input_dict.get("blue_collar", 0) * man_rate.get("blue collar (attendance)", 0)
        total_staff_cost += input_dict.get("loading_unloading", 0) * man_rate.get("loading & unloading(attendance)", 0)
        total_staff_cost += input_dict.get("electrition", 0) * man_rate.get("electretion", 0)
        total_staff_cost += input_dict.get("house_keeping", 0) * man_rate.get("house keeping", 0)
        total_staff_cost += input_dict.get("security_guard", 0) * man_rate.get("security guard", 0)
        total_staff_cost += input_dict.get("security_supervisor", 0) * man_rate.get("security supervisor", 0)
        total_staff_cost += input_dict.get("overtime_blue_collar", 0) * man_ot.get("blue collar (attendance)", 0)
        total_staff_cost += input_dict.get("overtime_supervisor", 0) * man_ot.get("security supervisor", 0)

        # Add Fixed White Collar Cost
        total_staff_cost += 8784

        # Calculate Total Cost = Total Staff Cost + all other input expenses
        exclude_fields = [
            "blue_collar", "loading_unloading", "electrition", "house_keeping",
            "security_guard", "security_supervisor",
            "overtime_blue_collar", "overtime_supervisor",
            "outbound_cbm", "storage_day_cbm", "tea", "staff_welfare"
        ]

        other_costs = sum(
            value for key, value in input_dict.items() if key not in exclude_fields
        )

        total_cost = total_staff_cost + other_costs

        # Profit Calculations
        gross_profit = total_revenue - total_staff_cost
        net_profit = total_revenue - total_cost
        net_profit_margin = (net_profit / total_revenue * 100) if total_revenue > 0 else 0

        summary_data.append({
            'date': date,
            'customer': customer,
            'location': location,
            'variable_revenue': round(variable_revenue, 2),
            'total_revenue': round(total_revenue, 2),
            'total_staff_cost': round(total_staff_cost, 2),
            'total_cost': round(total_cost, 2),
            'gross_profit': round(gross_profit, 2),
            'net_profit': round(net_profit, 2),
            'net_profit_margin': round(net_profit_margin, 2)
        })

    return render_template('summary.html', summary_data=summary_data)





@app.route('/api/summary_filter', methods=['GET'])
def api_summary_filter():
    """New API endpoint for dynamic filtering using AJAX."""
    try:
        date_filter = request.args.get('date', '')
        customer_filter = request.args.get('customer', '')
        location_filter = request.args.get('location', '')

        # Calculate P&L using the DB-driven function with filters
        summary_data = calculate_pl_summary_db(
            date_filter=date_filter,
            customer_filter=customer_filter,
            location_filter=location_filter
        )

        # Convert Decimals to strings for JSON serialization
        for item in summary_data:
            for key, value in item.items():
                if isinstance(value, Decimal):
                    item[key] = str(value)

        return jsonify(summary_data)

    except Exception as e:
        print(f"Error filtering summary data: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/config")
def config():
    return render_template("config.html")


# ------------------------
# RUN APP
# ------------------------
if __name__ == "__main__":
    app.run(debug=True)
